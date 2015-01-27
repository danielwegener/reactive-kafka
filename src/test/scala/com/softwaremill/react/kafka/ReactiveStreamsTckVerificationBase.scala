package com.softwaremill.react.kafka

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import org.jboss.netty.handler.codec.string.StringDecoder

trait ReactiveStreamsTckVerificationBase {

  implicit val system = ActorSystem()
  implicit val mat = FlowMaterializer()

  implicit val stringDecoder = new StringDecoder

  val kafka = new ReactiveKafka("localhost:9092", "localhost:2181")

  val message = ("foo","foo")
}

