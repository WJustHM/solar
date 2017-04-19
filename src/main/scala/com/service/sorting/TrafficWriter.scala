package com.service.sorting

import java.util
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import com.common.{BootStrap, Logging, Pools}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Put, RetriesExhaustedWithDetailsException}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.InputDStream
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import protocol.OffenceSnapDataProtos.OffenceSnapData

/**
  * Created by xuefei_wang on 17-3-13.
  */
class TrafficWriter extends BootStrap with Serializable{

  def getMetaData(item: OffenceSnapData,rowkey :Array[Byte]):Put ={
    val put = new Put(rowkey)
    put.addColumn(TrafficWriter.meta,"SBBH".getBytes,item.getId.getBytes)
    put.addColumn(TrafficWriter.meta,"DriveChan".getBytes,String.valueOf(item.getDriveChan).getBytes)
    put.addColumn(TrafficWriter.meta,"Time".getBytes,String.valueOf(item.getTime).getBytes)
    put.addColumn(TrafficWriter.meta,"AlarmDataType".getBytes,String.valueOf(item.getAlarmDataType).getBytes)
    put.addColumn(TrafficWriter.meta,"CopilotSafebelt".getBytes,String.valueOf(item.getCopilotSafebelt).getBytes)
    put.addColumn(TrafficWriter.meta,"CopilotSubVisor".getBytes,String.valueOf(item.getCopilotSubVisor).getBytes)
    put.addColumn(TrafficWriter.meta,"DetectType".getBytes,String.valueOf(item.getDetectType).getBytes)
    put.addColumn(TrafficWriter.meta,"Dir".getBytes,String.valueOf(item.getDir).getBytes)
    put.addColumn(TrafficWriter.meta,"IllegalType".getBytes,String.valueOf(item.getIllegalType).getBytes)
    put.addColumn(TrafficWriter.meta,"IllegalSubType".getBytes,String.valueOf(item.getIllegalSubType).getBytes)
    put.addColumn(TrafficWriter.meta,"MonitoringSiteID".getBytes,String.valueOf(item.getMonitoringSiteID).getBytes)
    put.addColumn(TrafficWriter.meta,"PicNum".getBytes,String.valueOf(item.getPicNum).getBytes)
    put.addColumn(TrafficWriter.meta,"PilotSafebelt".getBytes,String.valueOf(item.getPilotSafebelt).getBytes)
    put.addColumn(TrafficWriter.meta,"PilotSubVisor".getBytes,String.valueOf(item.getPilotSubVisor).getBytes)
    put.addColumn(TrafficWriter.meta,"SpeedLimit".getBytes,String.valueOf(item.getSpeedLimit).getBytes)
    put.addColumn(TrafficWriter.meta,"Vehicle_ChanIndex".getBytes,String.valueOf(item.getVehicleInfo.getChanIndex).getBytes)
    put.addColumn(TrafficWriter.meta,"Vehicle_Color".getBytes,String.valueOf(item.getVehicleInfo.getColor).getBytes)
    put.addColumn(TrafficWriter.meta,"Vehicle_Depth".getBytes,String.valueOf(item.getVehicleInfo.getColorDepth).getBytes)
    put.addColumn(TrafficWriter.meta,"Vehicle_Length".getBytes,String.valueOf(item.getVehicleInfo.getLength).getBytes)
    put.addColumn(TrafficWriter.meta,"Vehicle_Speed".getBytes,String.valueOf(item.getVehicleInfo.getSpeed).getBytes)
    put.addColumn(TrafficWriter.meta,"Vehicle_Attribute".getBytes,String.valueOf(item.getVehicleInfo.getVehicleAttribute).getBytes)
    put.addColumn(TrafficWriter.meta,"Vehicle_VehicleLogoRecog".getBytes,String.valueOf(item.getVehicleInfo.getVehicleLogoRecog).getBytes)
    put.addColumn(TrafficWriter.meta,"Vehicle_Type".getBytes,String.valueOf(item.getVehicleInfo.getVehicleType).getBytes)
    put.addColumn(TrafficWriter.meta,"Plate_Believe".getBytes,String.valueOf(item.getPlateInfo.getBelieve).getBytes)
    put.addColumn(TrafficWriter.meta,"Plate_Bright".getBytes,String.valueOf(item.getPlateInfo.getBright).getBytes)
    put.addColumn(TrafficWriter.meta,"Plate_Color".getBytes,String.valueOf(item.getPlateInfo.getColor).getBytes)
    put.addColumn(TrafficWriter.meta,"Plate_Country".getBytes,String.valueOf(item.getPlateInfo.getCountry).getBytes)
    put.addColumn(TrafficWriter.meta,"Plate_License".getBytes,String.valueOf(item.getPlateInfo.getLicense).getBytes)
    put.addColumn(TrafficWriter.meta,"Plate_PlateType".getBytes,String.valueOf(item.getPlateInfo.getPlateType).getBytes)
    put
  }
  def getImageData(item: OffenceSnapData,rowkey :Array[Byte]):Put = {
    val put = new Put(rowkey)
    val pics = item.getPicInfoList.iterator()
    while (pics.hasNext){
      val pic = pics.next()
      put.addColumn(TrafficWriter.image,"Data".getBytes,pic.getData.toByteArray)
      put.addColumn(TrafficWriter.image,"RecogMode".getBytes,String.valueOf(pic.getPicRecogMode).getBytes)
      put.addColumn(TrafficWriter.image,"RedLightTime".getBytes,String.valueOf(pic.getRedLightTime).getBytes)
    }
    put
  }
  def getRowkey(item: OffenceSnapData ,key : Long): Array[Byte] ={
    val time = item.getTime.replace("-","").replace(":","").replace(" ","").reverse
    val kkbh = item.getId
    val hphm = item.getPlateInfo.getLicense
    val rowkey = String.valueOf(key).reverse + "_"+hphm+"_"+kkbh
    rowkey.getBytes
  }


  def getRowkey( partitionId: Int, l: Long) = {
    val row = String.valueOf(l).reverse+"_"+partitionId
    row.getBytes
  }

  def getIndexReques(item: OffenceSnapData, rowkey: Array[Byte]):IndexRequest = {
    val indexRequest = new IndexRequest(TrafficWriter.ES_INDEX,TrafficWriter.ES_TYPY)
    val data = new util.HashMap[String,String]()
    data.put("SBBH",item.getId)
    data.put("DriveChan",String.valueOf(item.getDriveChan))
    data.put("Dir",String.valueOf(item.getDir))
    data.put("Vehicle_Color",String.valueOf(item.getVehicleInfo.getColor))
    data.put("Vehicle_ChanIndex",String.valueOf(item.getVehicleInfo.getChanIndex))
    data.put("Vehicle_Type",String.valueOf(item.getVehicleInfo.getVehicleType))
    data.put("IllegalType",String.valueOf(item.getIllegalType))
    data.put("Vehicle_Speed",String.valueOf(item.getVehicleInfo.getSpeed))
    data.put("Plate_License",String.valueOf(item.getPlateInfo.getLicense))
    data.put("Plate_PlateType",String.valueOf(item.getPlateInfo.getPlateType))
    data.put("Plate_Color",String.valueOf(item.getPlateInfo.getColor))
    data.put("Time",item.getTime)
    data.put("RowKey",new String(rowkey))
    indexRequest.source(data)
    indexRequest
  }




  override def setUpDstream(dstream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit = {
    val streams = dstream.map(r => OffenceSnapData.parseFrom(r.value()))
    streams.foreachRDD((r,time) => {
       r.foreachPartition(d => {
         if (!d.isEmpty){
           val hbaseConnA = TrafficWriter.getHbaseConn
           val hbaseConnB = TrafficWriter.getHbaseConn
           val metaPuts = new util.ArrayList[Put]()
           val imagePuts = new util.ArrayList[Put]()
           val timeer = new AtomicLong(System.nanoTime())
           val partitionId = TaskContext.get.partitionId()
           val listener_traffic =  new BufferedMutator.ExceptionListener(){
             override def onException(exception: RetriesExhaustedWithDetailsException, mutator: BufferedMutator): Unit = {
               println("Traffic exception "+exception.getMessage)
             }
           }
           val listener_traffic_image =  new BufferedMutator.ExceptionListener(){
             override def onException(exception: RetriesExhaustedWithDetailsException, mutator: BufferedMutator): Unit = {
               println("Traffic exception "+exception.getMessage)
             }
           }
           val params_Traffic = new BufferedMutatorParams(TrafficWriter.HbaseTableTraffic).listener(listener_traffic)
           val params_Traffic_Image = new BufferedMutatorParams(TrafficWriter.HbaseTableTrafficImage).listener(listener_traffic_image)
           val bufferedMutator_traffic  = hbaseConnA.getBufferedMutator(params_Traffic)
           val bufferedMutator_traffic_image  = hbaseConnB.getBufferedMutator(params_Traffic_Image)
           val esclient = TrafficWriter.getEsClient
           val esclientBuk = esclient.prepareBulk()
           d.foreach(item => {
             val rowkey = getRowkey(partitionId,timeer.getAndIncrement())
             metaPuts.add(getMetaData(item,rowkey))
             imagePuts.add(getImageData(item,rowkey))
             esclientBuk.add(getIndexReques(item,rowkey))
           })
           TrafficWriter.excutorpools.submit(new Runnable {
             override def run(): Unit = {
               bufferedMutator_traffic.mutate(metaPuts)
               bufferedMutator_traffic.close()
               metaPuts.clear()
               TrafficWriter.returnHbaseConn(hbaseConnA)
             }
           })
           TrafficWriter.excutorpools.submit(new Runnable {
             override def run(): Unit = {
               bufferedMutator_traffic_image.mutate(imagePuts)
               bufferedMutator_traffic_image.close()
               imagePuts.clear()
               TrafficWriter.returnHbaseConn(hbaseConnB)
             }
           })
           TrafficWriter.excutorpools.submit(new Runnable {
             override def run(): Unit = {
               esclientBuk.execute(TrafficWriter.esListener)
               TrafficWriter.returnEsConn(esclient)
             }
           })
         }
      })
    })
  }
  override def setAppName(name: String): Unit = {
    this.AppName = name
  }
}
object TrafficWriter extends Logging with Pools{


  val esListener = new ActionListener[BulkResponse](){
    override def onFailure(e: Exception): Unit = {
      println("ES failure ==>"  +e.toString)
    }
    override def onResponse(response: BulkResponse): Unit = {
      logInfo(response.toString)
    }
  }

  val HbaseTableTraffic = TableName.valueOf("Traffic")
  val HbaseTableTrafficImage = TableName.valueOf("TrafficImage")
  val ES_INDEX = "traffic"
  val ES_TYPY = "traffic"
  val meta = "MetaData".getBytes
  val image = "Image".getBytes
  val excutorpools = Executors.newWorkStealingPool(40)

  def main(args: Array[String]): Unit = {
    val trafficWriter = new TrafficWriter
    trafficWriter.setAppName("TrafficWriter")
    trafficWriter.run()
  }
}
