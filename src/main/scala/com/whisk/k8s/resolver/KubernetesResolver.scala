package com.whisk.k8s.resolver

import java.nio.file.{Files, Paths}

import com.twitter.finagle._
import com.twitter.logging.Logger
import com.twitter.util._

/**
  * Logical endpoint for service defined in Kubernetes,
  * which need to be resolved into actual ip addresses
  *
  * @param namespace   - for example, "prod"
  * @param serviceName - for example, "product-matching-thrift"
  */
case class Endpoint(namespace: String, serviceName: String)

class KubernetesResolver(defaultNamespace: String, endpointWatch: EndpointWatch) extends Resolver {

  def this() =
    this(
      defaultNamespace = new String(
        Files.readAllBytes(
          Paths.get(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
          )
        ),
        "utf-8"
      ),
      endpointWatch =
        new PollingEndpointWatch(Duration.fromSeconds(3), () => KubernetesClient.fromEnv)
    )

  private val logger = Logger.get(getClass)

  override val scheme: String = "k8s"

  override def bind(arg: String): Var[Addr] = {
    parseArg(arg) match {
      case Return(endpoint) => bindEndpointToAddr(endpoint)
      case Throw(exc)       => Var.value(Addr.Failed(exc))
    }
  }

  /**
    * Parse service destination in format "namespace/serviceName" or "serviceName"
    * for example "prod/product-matching-thrift" or "product-matching-thrift"
    * defaultNamespace is a namespace current service belongs to
    */
  private def parseArg(arg: String): Try[Endpoint] =
    Try {
      def fail() =
        throw new IllegalArgumentException(
          s"Endpoint specification " +
            s"should be 'namespace/serviceName:port' or 'serviceName:port', not '$arg'"
        )

      arg.split("/") match {
        case Array(namespace, serviceName) =>
          Endpoint(namespace, serviceName)

        case Array(serviceName) =>
          Endpoint(defaultNamespace, serviceName)

        case _ => fail()
      }
    }

  /**
    * Watch Kubernetes resource to receive changes of requested endpoints
    */
  private def bindEndpointToAddr(endpoint: Endpoint): Var[Addr] = {
    Var.apply(
      Addr.Pending,
      endpointWatch.watch(endpoint).map { addresses =>
        logger.info(s"Resolved addresses for $endpoint: $addresses")
        if (addresses.isEmpty) {
          Addr.Neg
        } else {
          Addr.Bound(addresses: _*)
        }
      }
    )
  }
}
