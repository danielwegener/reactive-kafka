package com.softwaremill.react.kafka

import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import kafka.producer.KafkaProducer

private[kafka] class KafkaActorSubscriber[K,V](val producer: KafkaProducer[K,V]) extends ActorSubscriber {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  def receive = {
    case ActorSubscriberMessage.OnNext(element:(K,V)) =>
      processElement(element)
    case ActorSubscriberMessage.OnError(ex) =>
      handleError(ex)
    case ActorSubscriberMessage.OnComplete =>
      streamFinished()
  }

  private def processElement(element: (K,V)) = {
    producer.send(element._1, element._2)
  }

  private def handleError(ex: Throwable) = {
    producer.close()
  }

  private def streamFinished() = {
    producer.close()
  }
}
