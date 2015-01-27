package com.softwaremill.react.kafka

import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import kafka.consumer.{ConsumerTimeoutException, KafkaConsumer}

import scala.util.{Failure, Success, Try}
private[kafka] class KafkaActorPublisher[K,V](consumer: KafkaConsumer[K,V]) extends ActorPublisher[(K,V)] {

  val iterator = consumer.iterator()

  override def receive = {
    case ActorPublisherMessage.Request(_) => read()
    case ActorPublisherMessage.Cancel | ActorPublisherMessage.SubscriptionTimeoutExceeded => cleanupResources()
  }

  private def read() {
    if (totalDemand < 0 && isActive)
      onError(new IllegalStateException("3.17: Overflow"))
    else readDemandedItems()
  }

  private def tryReadingSingleElement() = {
    Try{val nxt = iterator.next(); (nxt.key(),nxt.message())}.map(Some.apply).recover {
      // We handle timeout exceptions as normal 'end of the queue' cases
      case _: ConsumerTimeoutException => None
    }
  }

  private def readDemandedItems() {
    var maybeMoreElements = true
    while (isActive && totalDemand > 0 && maybeMoreElements) {
      tryReadingSingleElement() match {
        case Success(None) => maybeMoreElements = false // No more elements
        case Success(Some((key,message))) =>
            onNext((key, message))
          maybeMoreElements = true
        case Failure(ex) => onError(ex)
      }
    }

  }
  private def cleanupResources() {
    consumer.close()
  }
}