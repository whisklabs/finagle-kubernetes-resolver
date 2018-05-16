package com.whisk.k8s.resolver

import com.twitter.finagle.Address
import com.twitter.util.Event

trait EndpointWatch {
  def watch(endpoint: Endpoint): Event[Seq[Address]]
}
