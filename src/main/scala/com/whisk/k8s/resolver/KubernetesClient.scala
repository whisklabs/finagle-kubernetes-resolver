package com.whisk.k8s.resolver

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.twitter.finagle._
import com.twitter.finagle.http.{Chunk, Request, Response}
import com.twitter.finagle.ssl.TrustCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, Reader}
import com.twitter.logging.Logger
import com.twitter.util._
import io.circe
import io.circe.Decoder
import io.circe.parser.decode

object K8sApiModel {

  case class Address(ip: String)

  case class Port(port: Int, name: Option[String])

  case class Subset(addresses: Seq[Address], ports: Seq[Port])

  case class Metadata(resourceVersion: String)

  case class Endpoints(subsets: Seq[Subset], metadata: Metadata)

  case class Change(`object`: Endpoints)

  implicit val addressDecoder: Decoder[Address] = Decoder.forProduct1("ip")(Address)
  implicit val portDecoder: Decoder[Port] = Decoder.forProduct2("port", "name")(Port)
  implicit val subsetDecoder: Decoder[Subset] = Decoder.forProduct2("addresses", "ports")(Subset)
  implicit val metadataDecoder: Decoder[Metadata] = Decoder.forProduct1("resourceVersion")(Metadata)
  implicit val endpointsDecoder: Decoder[Endpoints] =
    Decoder.forProduct2("subsets", "metadata")(Endpoints)
  implicit val changeDecoder: Decoder[Change] = Decoder.forProduct1("object")(Change)

  private val logger = Logger.get(getClass)

  def parseEndpoints(buf: Buf): Either[circe.Error, Endpoints] = {
    val payload = Buf.decodeString(buf, StandardCharsets.UTF_8)
    logger.debug(payload)

    decode[Endpoints](payload)
  }

  def parseChange(buf: Buf): Either[circe.Error, Change] = {
    val payload = Buf.decodeString(buf, StandardCharsets.UTF_8)
    logger.debug(payload)

    decode[Change](payload)
  }
}

/**
  * Wrapper for kubernetes rest api
  */
class KubernetesClient(client: Service[Request, Response]) extends Closable {

  /**
    * Get pods ip addresses for a endpoint of service defined in Kubernetes
    */
  def getAddresses(endpoint: Endpoint): Future[(String, Seq[Address])] = {
    val namespace = endpoint.namespace
    val service = endpoint.serviceName
    val url = s"/api/v1/namespaces/$namespace/endpoints/$service"

    client(http.Request(http.Method.Get, url)).flatMap { response =>
      getContent(response)
        .map(K8sApiModel.parseEndpoints)
        .map(_.map(buildAddresses))
        .flatMap {
          case Left(error) => Future.exception(error)
          case Right((version, addresses)) =>
            Future.value((version, addresses))
        }
    }
  }

  /**
    * Watch changes for pods ip addresses for a endpoint of service defined in Kubernetes
    */
  def watchAddresses(endpoint: Endpoint, resourceVersion: String)(
    callback: Seq[Address] => Unit): Future[_] = {
    val namespace = endpoint.namespace
    val service = endpoint.serviceName
    val url = s"/api/v1/watch/namespaces/$namespace/endpoints/$service"

    val request = http.Request(url, ("watch", "true"), ("resourceVersion", resourceVersion))

    for {
      response <- client(request)
      _ <- ReaderUtil.readStream(response.reader, parseChunk)(callback)
    } yield {}
  }

  /**
    * Get content from the Kubernetes. If it's not chunked - get the content directly,
    * otherwise collect the content and then give it back to decoder as
    * circle.io can't process stream content.
    */
  private def getContent(response: Response): Future[Buf] = {
    if (response.isChunked) {

      //TODO: Trampoline this if recursion goes too deep
      def read(reader: Reader[Chunk], lastContent: Buf): Future[Buf] = reader.read().flatMap {
        case Some(chunk) =>
          val buf = lastContent.concat(chunk.content)
          read(reader, buf)
        case None => Future.value(lastContent)
      }

      read(response.chunkReader, Buf.Empty)
    } else {
      Future.value(response.content)
    }
  }


  private def parseChunk(buf: Buf): Either[circe.Error, Seq[Address]] = {
    K8sApiModel.parseChange(buf).map(change => buildAddresses(change.`object`)._2)
  }

  private def buildAddresses(endpoints: K8sApiModel.Endpoints): (String, Seq[Address]) = {
    val addresses = for {
      subset <- endpoints.subsets
      address <- buildAddressesFromSubset(subset)
    } yield address

    (endpoints.metadata.resourceVersion, addresses)
  }

  private def buildAddressesFromSubset(subset: K8sApiModel.Subset): Seq[Address] = {
    // If port with service name is not explicitly specified
    // then we check if service has only one port defined and point to it.

    if (subset.ports.length == 1) {
      for {
        address <- subset.addresses
        port <- subset.ports
      } yield {
        Address(address.ip, port.port)
      }
    } else {
      for {
        address <- subset.addresses
        port <- subset.ports
        portName <- port.name if portName == "service"
      } yield {
        Address(address.ip, port.port)
      }
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    client.close(deadline)
  }
}

object KubernetesClient {
  lazy val fromEnv: KubernetesClient = {
    val k8sHost = System.getenv("KUBERNETES_SERVICE_HOST")
    val k8sPort = System.getenv("KUBERNETES_PORT_443_TCP_PORT").toInt
    val token = new String(
      Files.readAllBytes(
        Paths.get(
          "/var/run/secrets/kubernetes.io/serviceaccount/token"
        )))
    val cert = new File("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")

    new KubernetesClient(
      Http.client
        .withStreaming(enabled = true)
        .configured(
          Transport.ClientSsl(
            Some(SslClientConfiguration(
              hostname = Some(k8sHost),
              trustCredentials = TrustCredentials.CertCollection(cert)
            ))))
        .filtered(Filter.mk((req, svc) => {
          req.authorization = s"Bearer $token"
          svc(req)
        }))
        .newService(s"$k8sHost:$k8sPort"))
  }
}
