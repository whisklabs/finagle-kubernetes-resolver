package com.whisk.k8s.resolver

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Addr, Address, Service}
import com.twitter.io.Buf
import com.twitter.util._
import org.scalatest.FunSuite


class KubernetesResolverTest extends FunSuite {

  case class Port(port: Int, name: Option[String] = None)

  case class Subset(ips: Seq[String], ports: Seq[Port])

  def buildK8sResponse(subset: Subset): String = {
    def buildAddress(ip: String): String = {
      s"""
         |   {
         |       "ip": "$ip",
         |       "nodeName": "minikube",
         |       "targetRef": {
         |           "kind": "Pod",
         |           "name": "product-matching-thrift-759ff4b78c-fdlbn",
         |           "namespace": "default",
         |           "resourceVersion": "7699",
         |           "uid": "0a029135-384b-11e8-a7c0-0800275755c0"
         |       }
         |   }
       """.stripMargin
    }

    def buildPort(port: Port): String = {
      s"""
         |   {
         |       "port": ${port.port},
         |       ${port.name.map(name => s""""name": "$name",""").getOrElse("")}
         |       "protocol": "TCP"
         |   }
       """.stripMargin
    }

    s"""
       |{
       |    "apiVersion": "v1",
       |    "kind": "Endpoints",
       |    "metadata": {
       |        "creationTimestamp": "2018-04-04T20:59:07Z",
       |        "name": "product-matching-thrift",
       |        "namespace": "default",
       |        "resourceVersion": "7702",
       |        "selfLink": "/api/v1/namespaces/default/endpoints/product-matching-thrift",
       |        "uid": "03caaa20-384b-11e8-a7c0-0800275755c0"
       |    },
       |    "subsets": [
       |        {
       |            "addresses": [${subset.ips.map(buildAddress).mkString(",")}],
       |            "ports": [${subset.ports.map(buildPort).mkString(",")}]
       |        }
       |    ]
       |}
       |
          """.stripMargin
  }

  val timer = new MockTimer

  def buildResolver(service: String, subset: Subset): KubernetesResolver = {

    val mockClient = new Service[Request, Response] {
      override def apply(request: Request): Future[Response] = {
        val response = Response()
        response.content = Buf.Utf8(buildK8sResponse(subset))
        Future.value(response)
      }
    }
    new KubernetesResolver(
      defaultNamespace = "default",
      new PollingEndpointWatch(Duration.fromSeconds(3),
        () => new KubernetesClient(mockClient),
        timer)
    )
  }

  def checkResolve(subset: Subset, expected: Addr): Unit = {
    val serviceName = "product-matching-thrift"

    val resolver = buildResolver(serviceName, subset)
    val addr = resolver.bind(serviceName)
    Time.withCurrentTimeFrozen { timeControl =>
      timeControl.advance(Duration.fromSeconds(5))
      timer.tick()
      val result = Await.result(addr.changes.filter(_ != Addr.Pending).toFuture)
      assertResult(expected)(result)
    }
  }

  test("if ports == 1 then resolve it") {
    val defaultPortWithoutName = 50054

    checkResolve(
      subset = Subset(
        ips = Seq("172.17.0.4", "172.17.0.5"),
        ports = Seq(
          Port(
            port = defaultPortWithoutName
          )
        )
      ),
      expected = Addr.Bound(
        Set(
          Address("172.17.0.4", defaultPortWithoutName),
          Address("172.17.0.5", defaultPortWithoutName)
        )
      )
    )
  }

  test("if ports > 1 then resolve port with name 'service'") {
    val servicePort = 50054
    val metricsPort = 8888

    checkResolve(
      subset = Subset(
        ips = Seq("172.17.0.4", "172.17.0.5"),
        ports = Seq(
          Port(
            name = Some("service"),
            port = servicePort
          ),
          Port(
            name = Some("metrics"),
            port = metricsPort
          )
        )
      ),
      expected = Addr.Bound(
        Set(
          Address("172.17.0.4", servicePort),
          Address("172.17.0.5", servicePort)
        ))
    )
  }

  test("if ports > 1 and no 'service' port, then don't resolve any port") {
    val servicePortWithoutName = 50054
    val metricsPort = 8888

    checkResolve(
      subset = Subset(
        ips = Seq("172.17.0.4", "172.17.0.5"),
        ports = Seq(
          Port(
            port = servicePortWithoutName
          ),
          Port(
            name = Some("metrics"),
            port = metricsPort
          )
        )
      ),
      expected = Addr.Neg
    )
  }

}
