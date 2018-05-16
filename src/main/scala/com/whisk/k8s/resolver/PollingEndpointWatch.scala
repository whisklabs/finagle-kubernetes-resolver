package com.whisk.k8s.resolver

import com.twitter.finagle.Address
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger
import com.twitter.util._

class PollingEndpointWatch(interval: Duration,
                           k8sClientFactory: () => KubernetesClient,
                           timer: Timer = DefaultTimer)
    extends EndpointWatch {

  private val logger = Logger.get(getClass)

  override def watch(endpoint: Endpoint): Event[Seq[Address]] = {
    val k8sClient = k8sClientFactory()

    val event = Event[Seq[Address]]()

    timer.schedule(Time.now.plus(interval), interval) {
      val future = k8sClient.getAddresses(endpoint).liftToTry.foreach {
        case Return((_, addresses)) =>
          event.notify(addresses)
        case Throw(e) =>
          logger.error(e, "Error in polling")
      }
      Await.result(future)
    }

    event.dedup
  }

}
