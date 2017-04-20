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

/**
  * Created by linux on 17-4-20.
  */
class SearchHBaseElasticsearch extends Pools {
  val tablename = TableName.valueOf("Result")
  val pla = Array("宝马", "奔驰", "法拉利", "保时捷", "兰博基尼", "林肯", "布加迪", "宾利", "阿斯顿马丁", "帕加尼")
  val month = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
  val hour = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
  val color = Array("蓝", "红", "黄", "绿")
  val ran = new Random()
  var rowkey = 0;

  def connElasetic(): TransportClient = {
    val client: TransportClient = getEsClient
    client
  }

  def search(): Unit = {
    val se = connElasetic().prepareGet("gxy", "gxy", "AVr-Dl0J-Iftm_7K0v-f").get()
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
    for (i <- 1 to 10) {
      val clientElastic: TransportClient = getEsClient
      val connHBase: HbaseConnection = getHbaseConn
      val bulkrequest = clientElastic.prepareBulk()
      val bufftable: BufferedMutator = connHBase.getBufferedMutator(tablename)
      for (j <- 1 to 100) {
        val mon = month(ran.nextInt(12))
        val time = "2017-" + mon + "-" + ran.nextInt(30) + " " + hour(ran.nextInt(12)) + ":23:12"
        val platecolor = color(ran.nextInt(4))
        val plate = pla(ran.nextInt(10))
        rowkey += 1

        //Elastic
        bulkrequest.add(clientElastic.prepareIndex("vehicle", "djj").setSource(
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

      }
      bufftable.flush()
      bufftable.close()
      bulkrequest.execute().get()
      returnHbaseConn(connHBase)
      returnEsConn(clientElastic)
      println("-------------" + i)
    }
  }

  def searchElasticHBase(start: String, end: String): Unit = {
    val client = getEsClient
    val stype = start.split(" ")(0).split("\\-")(1).toInt
    val etype = end.split(" ")(0).split("\\-")(1).toInt
    println(stype + ":" + etype)
    var request: SearchRequestBuilder = null
    request = client.prepareSearch().setIndices("vehicle").setTypes("djj")
    val qu = "{\n" +
      "    \"bool\": {\n" +
      "      \"must\": [\n" +
      "        {\n" +
      "          \"term\": {\n" +
      "            \"VehicleBrand.keyword\": {\n" +
      "              \"value\": \"阿斯顿马丁\"\n" +
      "            }\n" +
      "          }\n" +
      "        },\n" +
      "        {\n" +
      "          \"term\": {\n" +
      "            \"PlateColor.keyword\": {\n" +
      "              \"value\": \"红\"\n" +
      "            }\n" +
      "          }\n" +
      "        }\n" +
      "      ],\n" +
      "      \"filter\": {\n" +
      "        \"range\": {\n" +
      "          \"ResultTime\": {\n" +
      "            \"gte\": \"2017-01-20 15:37:02\",\n" +
      "            \"lte\": \"2017-12-20 15:37:02\"\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    }\n" +
      "  }"
    val response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu)).addSort("ResultTime", SortOrder.ASC).setSize(10000).execute().actionGet()
    for (i <- response.getHits.getHits) {
      i.getSource.get("resultRowkey")

    }
  }


  def searchHBase(rw:String): Unit = {
//    val conn: HbaseConnection = getHbaseConn
//    new Get(rw.getBytes)
  }
}

object SearchHBaseElasticsearch {
  def main(args: Array[String]): Unit = {
    val sbe = new SearchHBaseElasticsearch
    //    sbe.putDataHBaseElsaticserach
    sbe.searchElasticHBase("2017-07-09 08:23:12", "2017-12-09 08:23:12")
  }
}
