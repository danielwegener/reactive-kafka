package com.softwaremill.react.kafka

import java.util.UUID

import akka.stream.scaladsl.{PublisherSink, Source}
import org.reactivestreams.{Publisher, Subscriber}
import org.reactivestreams.tck.{SubscriberBlackboxVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class ReactiveKafkaSubscriberBlackboxSpec(defaultTimeout: FiniteDuration)
  extends SubscriberBlackboxVerification[(String,String)](new TestEnvironment(defaultTimeout.toMillis))
  with TestNGSuiteLike with ReactiveStreamsTckVerificationBase {

  def this() = this(300 millis)

  override def createSubscriber(): Subscriber[(String,String)] = {
    val topic = UUID.randomUUID().toString
    import ReactiveKafka._
    kafka.publish[String,String](topic, "group")
  }

  def createHelperSource(elements: Long): Source[(String,String)] = elements match {
    case 0 => Source.empty()
    case Long.MaxValue => Source(initialDelay = 10 millis, interval = 10 millis, () => message)
    case n if n <= Int.MaxValue => Source(List.fill(n.toInt)(message))
    case n => sys.error("n > Int.MaxValue")
  }

  def createHelperPublisher(elements: Long): Publisher[(String,String)] = {
    createHelperSource(elements).runWith(PublisherSink())
  }

}
