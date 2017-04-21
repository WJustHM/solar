package com.traffic.search

import com.common.Pools
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{BufferedMutator, Put, Table, Connection => HbaseConnection}
import org.apache.hadoop.hbase.util.Bytes
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random
import com.common.{BootStrap, Constants}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.common.xcontent.XContentFactory
import protocol.OffenceSnapDataProtos.OffenceSnapData

/**
  * Created by linux on 17-4-20.
  */
class SearchHBaseElasticsearch extends Pools {
  val clientElastic: TransportClient = getEsClient
  val connHBase: HbaseConnection = getHbaseConn
  val tablename = TableName.valueOf("Result")
  val bufftable: BufferedMutator = connHBase.getBufferedMutator(tablename)

  val pla = Array("宝马", "奔驰", "法拉利", "保时捷", "兰博基尼", "林肯", "布加迪", "宾利", "阿斯顿马丁", "帕加尼")
  val month = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
  val hour = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
  val color = Array("蓝", "红", "黄", "绿")
  val ran = new Random()
  var rowkey = 0;


  def clienttestxpack(): Unit ={
    val se = clientElastic.prepareGet(".kibana", "index-pattern", ".kibana").get()
    println(se.getSource)
  }


  def createHBaseTbale(connHBase: HbaseConnection): Unit = {
    val admin = connHBase.getAdmin
    if (admin.tableExists(tablename)) {
      println("Table exists!")
    } else {
      val tableDesc = new HTableDescriptor(tablename)
      tableDesc.addFamily(new HColumnDescriptor("Result".getBytes))
      admin.createTable(tableDesc)
      println("Create table success!")
    }
  }

  def putDataHBaseElsaticserach(): Unit = {
    val bulkrequest = clientElastic.prepareBulk()
    val bulkProcessor = BulkES(clientElastic)
    for (j <- 1 to 1000) {
      val mon = month(ran.nextInt(12))
      val time = "2017-" + mon + "-" + ran.nextInt(30) + " " + hour(ran.nextInt(12)) + ":23:12"
      val platecolor = color(ran.nextInt(4))
      val plate = pla(ran.nextInt(10))
      rowkey += 1

      //      bulkProcessor.add(new IndexRequest("twitter", "tweet").source(XContentFactory.jsonBuilder().startObject()
      //        .field("ResultTime", time)
      //        .field("PlateColor", platecolor)
      //        .field("Direction", 0)
      //        .field("VehicleBrand", plate)
      //        .field("tag", 1)
      //        .field("drop", 1)
      //        .field("secondBelt", 1)
      //        .field("call", 0)
      //        .field("crash", 0)
      //        .field("danger", 0)
      //        .field("paper", 0)
      //        .field("sun", 0)
      //        .field("resultRowkey", rowkey)
      //        .field("taskId", "007")
      //        .field("cameraId", "007")
      //        .field("datasourceId", "007")
      //        .endObject().string));


      // Elastic
      bulkrequest.add(clientElastic.prepareIndex("twitter", "djj").setSource(
        XContentFactory.jsonBuilder().startObject()
          .field("ResultTime", time)
          .field("PlateColor", platecolor)
          .field("Direction", 0)
          .field("VehicleBrand", plate)
          .field("tag", 1)
          .field("drop", 1)
          .field("secondBelt", 1)
          .field("call", 0)
          .field("crash", 0)
          .field("danger", 0)
          .field("paper", 0)
          .field("sun", 0)
          .field("resultRowkey", rowkey)
          .field("taskId", "007")
          .field("cameraId", "007")
          .field("datasourceId", "007")
          .endObject().string))

      //HBase
      val put: Put = new Put(Bytes.toBytes(rowkey + ""))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("taskid"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("cameraId"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("License"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("LicenseAttribution"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("PlateColor"), Bytes.toBytes(platecolor))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("PlateType"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("Confidence"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("Bright"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("Direction"), Bytes.toBytes(0))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("LocationLeft"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("LocationTop"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("LocationRight"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("LocationBottom"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("CostTime"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("CarBright"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("CarColor"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("CarLogo"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("ImagePath"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("ImageURL"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("ResultTime"), Bytes.toBytes(time))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("CreateTime"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("frame_index"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("carspeed"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("LabelInfoData"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleKind"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleBrand"), Bytes.toBytes(plate))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleSeries"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleStyle"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("tag"), Bytes.toBytes(1))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("paper"), Bytes.toBytes(0))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("sun"), Bytes.toBytes(0))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("drop"), Bytes.toBytes(1))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("call"), Bytes.toBytes(0))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("crash"), Bytes.toBytes(0))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("danger"), Bytes.toBytes(0))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("mainBelt"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("secondBelt"), Bytes.toBytes(1))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleLeft"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleTop"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleRight"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleBootom"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleConfidence"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("face"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("face_url1"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("face_url2"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("vehicleUrl"), Bytes.toBytes("007"))
      put.addColumn(Bytes.toBytes("Result"), Bytes.toBytes("fileName"), Bytes.toBytes("007"))
      bufftable.mutate(put)
      if (j % 100 == 0) {
        bufftable.flush()
        bulkrequest.execute().get()
        println("-------------" + j)
      }
    }
  }

  def BulkES(client: Client): BulkProcessor = {
    val bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener {
      override def beforeBulk(l: Long, bulkRequest: BulkRequest): Unit = {
        println("请求数量" + bulkRequest.numberOfActions())
      }

      override def afterBulk(l: Long, bulkRequest: BulkRequest, bulkResponse: BulkResponse): Unit = {
        if (bulkResponse.hasFailures) {
          println(bulkResponse.buildFailureMessage())
        }
      }

      override def afterBulk(l: Long, bulkRequest: BulkRequest, throwable: Throwable): Unit = {
        println("失败请求---------" + throwable.printStackTrace())
      }
    }).setBulkActions(100)
      .setFlushInterval(TimeValue.timeValueSeconds(5))
      .setConcurrentRequests(1)
      .setBackoffPolicy(
        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
      .build()
    bulkProcessor
  }
}

object SearchHBaseElasticsearch {
  def main(args: Array[String]): Unit = {
    val sbe = new SearchHBaseElasticsearch
//    sbe.putDataHBaseElsaticserach
    sbe.clienttestxpack
  }
}
