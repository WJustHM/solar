package com.common

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xuefei_wang on 17-2-27.
  */
trait Driver extends Logging {
   var AppName : String = Constants.APP_NAME
   private def driverRunner: Unit = {
    val sparkConf = initSparkConf
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sparkStreamContext = setUpStreamingContext(sparkContext)
    sparkStreamContext.start()
    sparkStreamContext.awaitTermination()
  }

  private def initSparkConf: SparkConf = {
    val sparkConf = new SparkConf().setAppName(getAppName)
    sparkConf.set("spark.streaming.kafka.consumer.poll.ms","20000")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "300")
    sparkConf
  }

  private def getKafkaParms: Map[String, String] = {
    var map: Map[String, String] = Map()
    map += ("bootstrap.servers" -> Constants.KAFKA_SERVER )
    map += ("key.deserializer" -> Constants.KAFKA_KEY_DESERIALIZER)
    map += ("value.deserializer" -> Constants.KAFKA_VALUE_DESERIALIZER)
    map += ("group.id" -> (Constants.KAFKA_GROUPID+String.valueOf(System.currentTimeMillis())))
    map += ("auto.offset.reset" -> Constants.KAFKA_AUTO_OFFSET_RESET)
    map += ("enable.auto.commit" -> Constants.KAFKA_ENABLE_AUTO_COMMIT)
    map
  }

  private def setUpStreamingContext(sparkContext: SparkContext): StreamingContext = {
    val duration = Constants.DURATION
    val streamingContext = StreamingContext.getActiveOrCreate(Constants.CHECKPOINT, () => new StreamingContext(sparkContext, Duration(duration)))
    streamingContext.checkpoint(Constants.CHECKPOINT)
    val topic = Array(Constants.KAFKA_TOPIC)
    println(" TOPICS :" + topic.mkString(","))
    println("-------------No Use Kafka offsets------------------")
    println("/////////////////////////////////////////////////////////////////////")
    println("////////// 智----能----交----通----大----数----据----平----台///////////")
    println("/////////////////////////////////////////////////////////////////////")
    val consumerStrategy: ConsumerStrategy[Array[Byte], Array[Byte]] = ConsumerStrategies.Subscribe[Array[Byte], Array[Byte]](topic, getKafkaParms)

    val dStream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]] = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      streamingContext,
      LocationStrategies.PreferConsistent,
      consumerStrategy)
    try {
      setUpDstream(dStream)
    } catch {
      case ex: Exception => {
        throw new RuntimeException(ex.getMessage)
      }
    }
    streamingContext
  }

   private def getAppName : String = AppName
   def setUpDstream(dstream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]])
   def setAppName(name : String)
   def run(): Unit ={
    driverRunner
  }

}


abstract class BootStrap extends Driver{

}