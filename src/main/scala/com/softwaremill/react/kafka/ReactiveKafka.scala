package com.softwaremill.react.kafka

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.stream.actor.{ActorSubscriber, ActorPublisher}
import kafka.consumer.KafkaConsumer
import kafka.producer.KafkaProducer
import kafka.serializer.{StringDecoder, StringEncoder, Decoder, Encoder}
import org.reactivestreams.{Publisher, Subscriber}

class ReactiveKafka(val host: String, val zooKeeperHost: String) {

  def publish[K,V](topic: String, groupId: String)(implicit actorSystem: ActorSystem, keySerializer:Class[_<:Encoder[K]], messageSerializer:Class[_<:Encoder[K]]): Subscriber[(K,V)] = {
    ActorSubscriber[(K,V)](producerActor(topic, groupId)(actorSystem, keySerializer, messageSerializer))
  }

  def producerActor[K,V](topic: String, groupId: String)(implicit actorSystem: ActorSystem, keySerializer:Class[_<:Encoder[K]], messageSerializer:Class[_<:Encoder[V]]): ActorRef = {
    val producer = new KafkaProducer[K,V](topic, host, keySerializer = keySerializer, messageSerializer = messageSerializer)
    actorSystem.actorOf(Props(new KafkaActorSubscriber(producer)).withDispatcher("kafka-subscriber-dispatcher"))
  }

  def consume[K,V](topic: String, groupId: String)(implicit actorSystem: ActorSystem, keyDecoder:Decoder[K], messageDecoder:Decoder[V]): Publisher[(K,V)] = {
    ActorPublisher[(K,V)](consumeAsActor(topic, groupId)(actorSystem, keyDecoder, messageDecoder))
  }

  def consumeAsActor[K,V](topic: String, groupId: String)(implicit actorSystem: ActorSystem, keyDecoder:Decoder[K], messageDecoder:Decoder[V]): ActorRef = {
    val consumer = new KafkaConsumer[K,V](topic, groupId, zooKeeperHost)
    actorSystem.actorOf(Props(new KafkaActorPublisher(consumer)).withDispatcher("kafka-publisher-dispatcher"))
  }

}

object ReactiveKafka {
  implicit val stringEncoder:Class[_<:Encoder[String]] = classOf[StringEncoder]
  implicit val stringDecoder:Decoder[String] = new StringDecoder
}





