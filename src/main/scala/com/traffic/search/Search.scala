package com.traffic.search

import com.common.Pools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortOrder
import org.apache.hadoop.hbase.client.{BufferedMutator, Get, Put, Table, Connection => HbaseConnection}
import org.elasticsearch.common.unit.TimeValue

/**
  * Created by linux on 17-4-20.
  */
class Search extends Pools {
  val conn: HbaseConnection = getHbaseConn//连接已经建立
  val client = getEsClient//连接以建立
  val tablename = TableName.valueOf("Result")
  val gettable = conn.getTable(tablename)
  var num = 0;

  def searchElasticHBase(start: String, end: String, PlateColor: String, vehicleBrand: String): Unit = {
    val stype = start.split(" ")(0).split("\\-")(1).toInt
    val etype = end.split(" ")(0).split("\\-")(1).toInt
    println(stype + ":" + etype)
    for (i <- stype to etype) {
      val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes(i.toString)
      val qu = "{\n" +
        "    \"bool\": {\n" +
        "      \"must\": [\n" +
        "        {\n" +
        "          \"term\": {\n" +
        "            \"vehicleBrand.keyword\": {\n" +
        "              \"value\": \"" + vehicleBrand + "\"\n" +
        "            }\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"term\": {\n" +
        "            \"PlateColor.keyword\": {\n" +
        "              \"value\": \"" + PlateColor + "\"\n" +
        "            }\n" +
        "          }\n" +
        "        }\n" +
        "      ],\n" +
        "      \"filter\": {\n" +
        "        \"range\": {\n" +
        "          \"ResultTime\": {\n" +
        "            \"gte\": \"" + start + "\",\n" +
        "            \"lte\": \"" + end + "\"\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }"
      val response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu)).addSort("ResultTime", SortOrder.ASC).setSize(10000).execute().actionGet()
      for (i <- response.getHits.getHits) {
        searchHBase(i.getSource.get("resultId").toString)
        num += 1
      }
      println("---------------------------------")
    }
    println("~:" + num)
  }

  def searchHBase(rw: String): Unit = {
    val get = new Get(rw.getBytes)
    val result = gettable.get(get)
    val License = Bytes.toString(result.getValue("Result".getBytes, "License".getBytes))
    val PlateType = Bytes.toString(result.getValue("Result".getBytes, "PlateType".getBytes))
    val PlateColor = Bytes.toString(result.getValue("Result".getBytes, "PlateColor".getBytes))
    val Confidence = Bytes.toString(result.getValue("Result".getBytes, "Confidence".getBytes))
    val LicenseAttribution = Bytes.toString(result.getValue("Result".getBytes, "LicenseAttribution".getBytes))
    val ImageURL = Bytes.toString(result.getValue("Result".getBytes, "ImageURL".getBytes))
    val CarColor = Bytes.toString(result.getValue("Result".getBytes, "CarColor".getBytes))
    val ResultTime = Bytes.toString(result.getValue("Result".getBytes, "ResultTime".getBytes))
    val Direction = Bytes.toString(result.getValue("Result".getBytes, "Direction".getBytes))
    val frame_index = Bytes.toString(result.getValue("Result".getBytes, "frame_index".getBytes))
    val vehicleKind = Bytes.toString(result.getValue("Result".getBytes, "vehicleKind".getBytes))
    val vehicleBrand = Bytes.toString(result.getValue("Result".getBytes, "vehicleBrand".getBytes))
    val vehicleStyle = Bytes.toString(result.getValue("Result".getBytes, "vehicleStyle".getBytes))
    val LocationLeft = Bytes.toString(result.getValue("Result".getBytes, "LocationLeft".getBytes))
    println("----" + result.toString)
  }

  def searchCarnumber(start: String, end: String): Unit = {
    val stype = start.split(" ")(0).split("\\-")(1).toInt
    val etype = end.split(" ")(0).split("\\-")(1).toInt
    println(stype + ":" + etype)
    var num = 0
    for (i <- stype to etype) {
      val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes(i.toString)
      val qu = "{\n" +
        "    \"range\": {\n" +
        "      \"ResultTime\": {\n" +
        "        \"gte\": \"" + start + "\",\n" +
        "        \"lte\": \"" + end + "\"\n" +
        "      }\n" +
        "    }\n" +
        "  }"
      var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu))
        .addSort("ResultTime", SortOrder.ASC).setSize(10000)
        .setScroll(new TimeValue(60000)).execute().actionGet()
      do {
        num += response.getHits.getHits.length
        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
      } while (response.getHits.getHits.length != 0)
    }
    println("~~~~~~:" + num)
  }
}

object Search {
  def main(args: Array[String]): Unit = {
    val sbe = new Search
    //    sbe.searchElasticHBase("2017-01-20 15:37:02", "2017-12-20 15:37:02", "白", "斯柯达")
    sbe.searchCarnumber("2016-02-20 15:30:14", "2016-06-20 15:30:14")
  }
}