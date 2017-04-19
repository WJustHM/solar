package com.service.statistics

/**
  * Created by xuefei_wang on 17-3-1.
  */
import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import com.common.{BootStrap, Constants, Pools}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Durability, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{Accumulator, SparkContext}
import protocol.OffenceSnapDataProtos.OffenceSnapData

import scala.collection.immutable.TreeMap
import scala.collection.mutable.HashMap


class Statistic extends BootStrap with Serializable {

  override def setUpDstream(dstream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit = {
    val streams = dstream.map(r => OffenceSnapData.parseFrom(r.value()))
    run(streams)
  }

  override def setAppName(name: String): Unit = {
    this.AppName = name
  }

  def run(dStreams: DStream[OffenceSnapData]) = {

    val msdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val mdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val hdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val ddf = new SimpleDateFormat("yyyy-MM-dd")

    //window实现流量统计
    val statisticsDStreams = dStreams.map(offenceSnapData => {
      val deviceId = offenceSnapData.getId
      val vehicleType = offenceSnapData.getVehicleInfo.getVehicleType
      val time = offenceSnapData.getTime
      val speed = offenceSnapData.getVehicleInfo.getSpeed
      val vehicleLength = offenceSnapData.getVehicleInfo.getLength
      var dfTime : Date = null
      try {
        dfTime = df.parse(time)
      } catch {
        case e: ParseException => dfTime = new Date()
      }
      val treeMap = TreeMap("M" -> TreeMap(vehicleType + "|" + mdf.format(dfTime) -> 1),
                            "H" -> TreeMap(vehicleType + "|" + hdf.format(dfTime) -> 1),
                            "D" -> TreeMap(vehicleType + "|" + ddf.format(dfTime) -> 1),
                            "S" -> TreeMap(mdf.format(dfTime) -> speed),
                            "L" -> TreeMap(mdf.format(dfTime) -> vehicleLength))
      (deviceId, treeMap)
    })

    //按卡口统计每分钟，每小时，每天的3个不同粒度的每种车型的车流量，一个卡口一条记录
    //形如(426,Map(D -> Map(3|2016-12-13 -> 570，2|2016-12-14 -> 400, ...), H -> Map(3|2016-12-13 14 -> 570，2|2016-12-14 12 -> 400, ...), ...)）
    val statisticsResult = statisticsDStreams.reduceByKeyAndWindow(
      (x1,x2) => {
        var x3 = x1
        x2("D").keys.foreach(key => {
          val dvalue =  x2("D")(key).asInstanceOf[Int]
          if(x1("D") contains key) {
            x3 = x3 + ("D" -> (x3("D") + (key -> (x3("D")(key).asInstanceOf[Int] + dvalue))))
          } else {
            x3 = x3 + ("D" -> (x3("D") + (key -> dvalue)))
          }
        })
        x2("H").keys.foreach(key => {
          val hvalue = x2("H")(key).asInstanceOf[Int]
          if(x1("H") contains key) {
            x3 = x3 + ("H" -> (x3("H") + (key -> (x3("H")(key).asInstanceOf[Int] + hvalue))))
          } else {
            x3 = x3 + ("H" -> (x3("H") + (key -> hvalue)))
          }
        })
        x2("M").keys.foreach(key => {
          val mvalue = x2("M")(key).asInstanceOf[Int]
          if(x1("M") contains key) {
            x3 = x3 + ("M" -> (x3("M") + (key -> (x3("M")(key).asInstanceOf[Int] + mvalue))))
          } else {
            x3 = x3 + ("M" -> (x3("M") + (key -> mvalue)))
          }
        })
        x2("L").keys.foreach(key => {
          val lvalue = x2("L")(key).asInstanceOf[Float]
          if (x1("L") contains key) {
            x3 = x3 + ("L" -> (x3("L") + (key -> ((x3("L")(key).asInstanceOf[Float] + lvalue) / 2 * 100).toInt / 100f)))
          } else {
            x3 = x3 + ("L" -> (x3("L") + (key -> lvalue)))
          }
        })
        x2("S").keys.foreach(key => {
          val svalue = x2("S")(key).asInstanceOf[Float]
          if (x1("S") contains key) {
            x3 = x3 + ("S" -> (x3("S") + (key -> ((x3("S")(key).asInstanceOf[Float] + svalue) / 2 * 100).toInt / 100f)))
          } else {
            x3 = x3 + ("S" -> (x3("S") + (key -> svalue)))
          }
        })
        x3
      },
      (y1,y2) => {
        var y3 = y1
        y2("M").keys.foreach(oldKey => {
          if(y1("M") contains oldKey) {
            y3 = y3 + ("M" -> (y3("M") - oldKey))
          }
        })
        y2("H").keys.foreach(oldKey => {
          val time = oldKey.split('|')(1)
          val lastHour = hdf.parse(time).getTime - 3600000l
          y1("H").keys.foreach(key => {
            if(hdf.parse(key.split('|')(1)).getTime <= lastHour){
              y3 = y3 + ("H" -> (y3("H") - key))
            }
          })
        })
        y2("D").keys.foreach(oldKey => {
          val time = oldKey.split('|')(1)
          val lastDay = ddf.parse(time).getTime - 86400000l
          y1("D").keys.foreach(key => {
            if(ddf.parse(key.split('|')(1)).getTime <= lastDay){
              y3 = y3 + ("D" -> (y3("D") - key))
            }
          })
        })
        y2("L").keys.foreach(oldKey => {
          if (y1("L") contains oldKey) {
            y3 = y3 + ("L" -> (y3("L") - oldKey))
          }
        })
        y2("S").keys.foreach(oldKey => {
          if (y1("S") contains oldKey) {
            y3 = y3 + ("S" -> (y3("S") - oldKey))
          }
        })
        y3
      }, Seconds(600), Seconds(1))

    //把统计出来的数据格式化成一个卡口每天一条记录
    //形如(426|2016-12-13,Map(D -> Map(3|2016-12-13 -> 570), H -> Map(3|2016-12-13 14 -> 370，2|2016-12-13 15 -> 200, ...), ...)）
    //   (426|2016-12-14,Map(D -> Map(3|2016-12-14 -> 400), H -> Map(3|2016-12-13 14 -> 270，2|2016-12-13 15 -> 130, ...), ...)）
    val statisticsRF = statisticsResult.flatMap(data => {
      val flatDataMap = HashMap[String,HashMap[String,HashMap[String, AnyVal]]]()

      val deviceId = data._1
      val days = data._2("D")
      val hours = data._2("H")
      val minutes = data._2("M")
      val speeds = data._2("S")
      val lengths = data._2("L")

      days.map(day => {
        val key = deviceId + "|" + day._1.split('|')(1)
        if (flatDataMap contains key) {
          flatDataMap(key)("D") += day
        } else {
          val newDay = HashMap(day)
          flatDataMap += key -> HashMap[String, HashMap[String, AnyVal]]()
          flatDataMap(key) += ("D" -> newDay)
        }
      })

      hours.map(hour => {
        val key = deviceId +"|"+ hour._1.split("\\s")(0).split('|')(1)
        if (flatDataMap contains key) {
          if (flatDataMap(key) contains "H") {
            flatDataMap(key)("H") += hour
          } else {
            val newHour = HashMap(hour)
            flatDataMap(key) += ("H" -> newHour)
          }
        }
      })

      minutes.map(minute => {
        val key = deviceId +"|"+ minute._1.split("\\s")(0).split('|')(1)
        if (flatDataMap contains key) {
          if (flatDataMap(key) contains "M") {
            flatDataMap(key)("M") += minute
          } else {
            val newMinute = HashMap(minute)
            flatDataMap(key) += ("M" -> newMinute)
          }
        }
      })

      speeds.map(speed => {
        val key = deviceId + "|" + speed._1.split("\\s")(0)
        if (flatDataMap contains key) {
          if (flatDataMap(key) contains "S") {
            flatDataMap(key)("S") += speed
          } else {
            val newSpeed = HashMap(speed)
            flatDataMap(key) += ("S" -> newSpeed)
          }
        }
      })

      lengths.map(length => {
        val key = deviceId + "|" + length._1.split("\\s")(0)
        if (flatDataMap contains key) {
          if (flatDataMap(key) contains "L") {
            flatDataMap(key)("L") += length
          } else {
            val newLength = HashMap(length)
            flatDataMap(key) += ("L" -> newLength)
          }
        }
      })

      flatDataMap
    })

    statisticsRF.print()

    statisticsRF.foreachRDD(rdd => {
//      val toHBaseCounter = Statistic.getInstance(rdd.sparkContext)
//      println("----------------toHBaseCounter:" + toHBaseCounter.value + "-----------------")
//      if (toHBaseCounter.value == 119) {
        println("----------------insert statistics data into hbase-----------------")
        rdd.foreachPartition(part => {
          val hbaseConn = Statistic.getHbaseConn
          val hbaseTable = hbaseConn.getBufferedMutator(TableName.valueOf(Constants.HBASE_TABLE_STATISTICS))
          part.foreach(data => {
            val infoPut = Statistic.convertToPut(data)
            hbaseTable.mutate(infoPut)
          })
          hbaseTable.flush()
          hbaseTable.close()
          Statistic.returnHbaseConn(hbaseConn)
          println("----------------insert statistics data into hbase end-----------------")
        })
//      } else rdd.foreachPartition(part => {})
//
//      toHBaseCounter.add(1)
    })
  }
}

object Statistic extends Pools with Serializable{

  @volatile private var instance: Accumulator[Int] = null
  @volatile private var instance2: Accumulator[Int] = null

  def getInstance(sc: SparkContext): Accumulator[Int] = {
    if (instance == null || instance.value == 120) {
      synchronized {
        if (instance == null || instance.value == 120) {
          instance = sc.accumulator(0, "ToHBaseCounter")
        }
      }
    }
    instance
  }
  def getInstance2(sc: SparkContext): Accumulator[Int] = {
    if (instance2 == null || instance2.value == 10) {
      synchronized {
        if (instance2 == null || instance2.value == 10) {
          instance2 = sc.accumulator(0, "ToRedisCounter")
        }
      }
    }
    instance2
  }

  def convertToPut(data: (String, HashMap[String, HashMap[String, AnyVal]])): Put = {
    val ddf = new SimpleDateFormat("yyyy-MM-dd")
    val hdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val mdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val key = data._1
    val value = data._2
    val Array(deviceId: String, day: String) = key.split('|')
    val partitionNo = deviceId.charAt(deviceId.length-1).toInt % 3
    val rowkey = "%d|%s_%s".format(partitionNo,deviceId,day)
    val put = new Put(Bytes.toBytes(rowkey))

    var dayCount: String = ""
    value("D").foreach(v => {
      dayCount += v._1.split('|')(0) +":"+ v._2+"|"
    })
    put.addColumn(Bytes.toBytes(Constants.STATISTICS_FAMILY_NAME), Bytes.toBytes("D"), Bytes.toBytes(dayCount))

    if(value.getOrElse("H", null) != null){
      val hourCount = new HashMap[Int,String]
      value("H").foreach(v => {
        val Array(vehicleType: String, time: String) = v._1.split('|')
        val hour = (hdf.parse(time).getTime - ddf.parse(day).getTime) / 60 / 60 / 1000
        if(hourCount contains hour.toInt) hourCount(hour.toInt) += vehicleType +":"+ v._2 +"|"
        else hourCount(hour.toInt) = vehicleType +":"+ v._2 +"|"
      })
      for(perHour <- hourCount){
        put.addColumn(Bytes.toBytes(Constants.STATISTICS_FAMILY_NAME), Bytes.toBytes(perHour._1.toString + "h"), Bytes.toBytes(perHour._2))
      }
    }

    if(value.getOrElse("M", null) != null){
      val minuteCount = new HashMap[Int,String]
      value("M").foreach(v => {
        val Array(vehicleType: String, time: String) = v._1.split('|')
        val minute = (mdf.parse(time).getTime - ddf.parse(day).getTime) / 60 / 1000
        if(minuteCount contains minute.toInt) minuteCount(minute.toInt) += vehicleType +":"+ v._2 +"|"
        else minuteCount(minute.toInt) = vehicleType +":"+ v._2 +"|"
      })
      for(perMinute <- minuteCount){
        put.addColumn(Bytes.toBytes(Constants.STATISTICS_FAMILY_NAME), Bytes.toBytes(perMinute._1.toString), Bytes.toBytes(perMinute._2))
      }
    }

    if(value.getOrElse("S", null) != null){
      val speedCount = new HashMap[Int,String]
      value("S").map(v => {
        val time = v._1
        val speed = v._2
        val minute = (mdf.parse(time).getTime - ddf.parse(day).getTime) / 60 / 1000
        speedCount += (minute.toInt -> speed.toString)
      })
      for(perMinute <- speedCount){
        put.addColumn(Bytes.toBytes(Constants.STATISTICS_FAMILY_NAME), Bytes.toBytes(perMinute._1 + "S"), Bytes.toBytes(perMinute._2))
      }
    }

    if(value.getOrElse("L", null) != null){
      val vehicleLengthCount = new HashMap[Int,String]
      value("L").map(v => {
        val time = v._1
        val vehicleLength = v._2
        val minute = (mdf.parse(time).getTime - ddf.parse(day).getTime) / 60 / 1000
        vehicleLengthCount += (minute.toInt -> vehicleLength.toString)
      })
      for(perMinute <- vehicleLengthCount){
        put.addColumn(Bytes.toBytes(Constants.STATISTICS_FAMILY_NAME), Bytes.toBytes(perMinute._1 + "L"), Bytes.toBytes(perMinute._2))
      }
    }
    put.setDurability(Durability.SKIP_WAL)

    put
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hbase")

    val stat = new Statistic()
    stat.setAppName("Statistic")
    stat.run()
  }
}





