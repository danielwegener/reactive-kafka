package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import kafka.serializer.{Encoder, StringEncoder, StringDecoder}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike
with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ReactiveKafkaIntegrationSpec"))

  val topic = UUID.randomUUID().toString
  val group = "group"
  implicit val timeout = Timeout(1 second)
  implicit val stringDecoder = new StringDecoder()
  implicit val stringSerializer = classOf[StringEncoder]

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Reactive kafka streams" must {

    "combine well" in {
      // given
      val kafka = new ReactiveKafka("localhost:9092", "localhost:2181")
      val publisher = kafka.consume[String,String](topic, group)
      val kafkaSubscriber = kafka.publish[String,String](topic, group)
      val subscriberActor = system.actorOf(Props(new ReactiveTestSubscriber))
      val testSubscriber = ActorSubscriber[(String,String)](subscriberActor)
      publisher.subscribe(testSubscriber)

      // when
      kafkaSubscriber.onNext(("one","one"))
      kafkaSubscriber.onNext(("two","two"))

      // then
      awaitCond {
        val collectedStrings = Await.result(subscriberActor ? "get elements", atMost = 1 second)
        collectedStrings == List("one", "two")
      }
    }
  }
}

class ReactiveTestSubscriber extends ActorSubscriber {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  var elements: Vector[(String,String)] = Vector.empty

  def receive = {
    case ActorSubscriberMessage.OnNext(element) => elements = elements :+ element.asInstanceOf[(String,String)]
    case "get elements" => sender ! elements
  }
}