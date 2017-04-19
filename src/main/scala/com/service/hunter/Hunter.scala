package com.service.hunter

import java.io.{ByteArrayOutputStream, Serializable}
import java.util
import java.util.concurrent.ConcurrentHashMap

import com.common.{BootStrap, Constants, Logging, Pools}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Connection => HbaseConnection}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.spark.streaming.dstream.InputDStream
import protocol.AlarmInfoProtos
import protocol.OffenceSnapDataProtos.OffenceSnapData

class Hunter extends  BootStrap with Serializable{

  override def setUpDstream(dstream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit = {
    val streams = dstream.map(r => OffenceSnapData.parseFrom(r.value()))
    streams.foreachRDD( d => {
      d.foreachPartition( p =>{
        val hbase = Hunter.getHbaseConn
        val kafka = Hunter.getKafkaConn
        p.foreach(data => {
          val license = data.getPlateInfo.getLicense
          val blackFilter = Hunter.blackInfoMap.keySet.contains(license)
          if(blackFilter) {
            val blackInfo = Hunter.blackInfoMap.get(license)
            Hunter.writeToKafka(kafka,blackInfo,data)
            Hunter.writeToHBase(hbase,blackInfo,data)
          }
      })
        Hunter.returnKafkaConn(kafka)
        Hunter.returnHbaseConn(hbase)
    })
   })
  }
  override def setAppName(name: String): Unit = {
    this.AppName = "hunterApp"
  }
}

object Hunter extends Logging with Pools{

  var blackInfoMap = new ConcurrentHashMap[String, ConcurrentHashMap[String, String]]

  def writeToKafka(producer:Producer[Array[Byte], Array[Byte]], blackMap: ConcurrentHashMap[String, String], data: OffenceSnapData): Unit = {
    //    ------------key为：时间＋车牌＋卡口号-----------
    val key = data.getTime + data.getPlateInfo.getLicense + data.getId
    val alarmDataBuilder = AlarmInfoProtos.AlarmInfo.newBuilder()
    alarmDataBuilder.setPriority(Integer.parseInt(blackMap.get("BKJB")))
    alarmDataBuilder.setPreyInfoId(blackMap.get("BKXXBH"))
    alarmDataBuilder.setId(data.getId+ data.getTime)
    alarmDataBuilder.setTime(data.getTime)
    alarmDataBuilder.setVehicleSpeed(data.getVehicleInfo.getSpeed)
    alarmDataBuilder.setPlateLicense(data.getPlateInfo.getLicense)
    alarmDataBuilder.setVehicleId(data.getPlateInfo.getLicense + data.getTime)
    alarmDataBuilder.setType(data.getAlarmDataType)
    alarmDataBuilder.setPlateColor(data.getPlateInfo.getColor)
    alarmDataBuilder.setPlateType(data.getPlateInfo.getPlateType)
    alarmDataBuilder.setVehicleLogoRecog(data.getVehicleInfo.getVehicleLogoRecog)
    alarmDataBuilder.setVehicleType(data.getVehicleInfo.getVehicleType)
    alarmDataBuilder.setDeviceId(data.getId)
    val alarmData = alarmDataBuilder.build()
    val _data = new ByteArrayOutputStream()
    alarmData.writeTo(_data)

    val message = new ProducerRecord[Array[Byte], Array[Byte]](Constants.KAFKA_ALARM_TOPIC,key.getBytes,_data.toByteArray)
    producer.send(message)
  }

  def writeToHBase(hbase: HbaseConnection,blackMap: ConcurrentHashMap[String, String],data: OffenceSnapData): Unit ={
    val tableName = TableName.valueOf("AlarmInfo")
    val table = hbase.getBufferedMutator(tableName)
    val puts : util.ArrayList[Put] = new util.ArrayList[Put]()
    val put = new Put(Bytes.toBytes(data.getTime + data.getPlateInfo.getLicense + data.getId))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("BKXXBH"), Bytes.toBytes(blackMap.get("BKXXBH")))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("BKJB"), Bytes.toBytes(blackMap.get("BKJB")))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("BKLX"), Bytes.toBytes(data.getAlarmDataType))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("HPYS"), Bytes.toBytes(data.getPlateInfo.getColor))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("HPZL"), Bytes.toBytes(data.getPlateInfo.getPlateType))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("CLPP1"), Bytes.toBytes(data.getVehicleInfo.getVehicleLogoRecog))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("CSYS"), Bytes.toBytes(data.getVehicleInfo.getColor))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("CLLX"), Bytes.toBytes(data.getVehicleInfo.getVehicleType))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("CLXXBH"), Bytes.toBytes(data.getPlateInfo.getLicense + data.getTime))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("KKBH"), Bytes.toBytes(data.getId))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("GCSJ"), Bytes.toBytes(data.getTime))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("CLSD"), Bytes.toBytes(data.getVehicleInfo.getSpeed))
    put.addColumn(Bytes.toBytes("AlarmInfo"), Bytes.toBytes("BJBH"), Bytes.toBytes(data.getId + data.getTime))
    puts.add(put)
    table.mutate(puts)
    table.flush
  }
  val sub = new SubManager
  sub.getBlackInfo()
  def main(args: Array[String]): Unit = {
    new Thread(new SubManager()).start()

    val hunter = new Hunter
    hunter.run()
  }
}