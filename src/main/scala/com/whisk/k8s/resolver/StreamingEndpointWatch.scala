package com.whisk.k8s.resolver

import com.twitter.conversions.time._
import com.twitter.finagle.Address
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger
import com.twitter.util._

class StreamingEndpointWatch(k8sClientFactory: () => KubernetesClient)(implicit timer: Timer =
                                                                         DefaultTimer)
    extends EndpointWatch {
  private val logger = Logger.get(getClass)

  /**
    * Watch changes for the endpoint:
    * 1) get current version of the endpoint
    * 2) subscribe to changes starting from this version
    *
    * This watch is infinite: if error happens we reconnect and start again
    */
  def watch(endpoint: Endpoint): Event[Seq[Address]] = {
    logger.info("Connecting to Kubernetes...")

    val event = Event[Seq[Address]]()

    def loop(): Unit = {

      val client = k8sClientFactory()

      def withRestartOnError(watching: Future[Unit]): Unit = {
        watching.handle({
          // recursive call to continue this watch with a new client
          case e =>
            logger.error(e, "Error in Kubernetes resolver, restarting...")
            terminate(client).foreach(_ => loop())
        })
      }

      withRestartOnError(for {
        (version, resource) <- client.getAddresses(endpoint)
        _ <- {
          event.notify(resource)
          client.watchAddresses(endpoint, version)(event.notify)
        }
      } yield {})

    }

    loop()

    event.dedup

  }

  private def terminate(previous: KubernetesClient): Future[_] = {
    def backOff(duration: Duration): Future[Unit] = {
      logger.info(s"Sleeping for $duration before reconnection...")
      Future.sleep(duration)
    }

    for {
      _ <- previous.close(5.seconds)
      _ <- backOff(5.seconds)
    } yield ()

  }

}
