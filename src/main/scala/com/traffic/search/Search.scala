package com.traffic.search

import java.text.SimpleDateFormat
import java.util._

import com.common.Pools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortOrder
import org.apache.hadoop.hbase.client.{BufferedMutator, Get, Put, Table, Connection => HbaseConnection}
import org.elasticsearch.common.unit.TimeValue
import redis.clients.jedis.{HostAndPort, JedisCluster}
import org.codehaus.jackson.map.ObjectMapper

/**
  * Created by linux on 17-4-20.
  */
class Search extends Pools {
  val conn: HbaseConnection = getHbaseConn
  //连接已经建立
  val client = getEsClient
  //连接以建立
  val redis = initRedis
  val tablename = TableName.valueOf("Result")
  val gettable = conn.getTable(tablename)
  val mapper = new ObjectMapper()
  val TASK = "TASK"
  val CAMERA = "CAMERA"
  val DATASOURCE = "DATASOURCE"
  var num = 0;

  def searchElasticHBase(start: String, end: String, PlateColor: String, vehicleBrand: String): Unit = {
    //    val stype = start.split(" ")(0).split("\\-")(1).toInt
    //    val etype = end.split(" ")(0).split("\\-")(1).toInt
    val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes("result")
    val qu = "{\n" +
      "    \"bool\": {\n" +
      "      \"must\": [\n" +
      "        {\n" +
      "          \"term\": {\n" +
      "            \"vehicleBrand.keyword\": {\n" +
      "              \"value\": \"丰田\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"PlateColor.keyword\": {\n" +
      "              \"value\": \"黄\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"Direction\": {\n" +
      "              \"value\": \"1\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"tag.keyword\": {\n" +
      "              \"value\": \"true\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"paper.keyword\": {\n" +
      "              \"value\": \"false\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"sun.keyword\": {\n" +
      "              \"value\": \"false\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"drop.keyword\": {\n" +
      "              \"value\": \"true\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"secondBelt.keyword\": {\n" +
      "              \"value\": \"true\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"crash.keyword\": {\n" +
      "              \"value\": \"true\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"danger.keyword\": {\n" +
      "              \"value\": \"false\"\n" +
      "            }\n" + "          }\n" + "        }\n" + "      ],\n" +
      "      \"filter\": {\n" +
      "        \"range\": {\n" +
      "          \"ResultTime\": {\n" +
      "            \"gte\": \"2017-04-10 15:37:02\",\n" +
      "            \"lte\": \"2017-04-20 15:37:02\"\n" +
      "          }\n" + "        }\n" + "      }\n" + "    }\n" +
      "  }"
    var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu))
      .addSort("ResultTime", SortOrder.ASC).setScroll(new TimeValue(60000)).setSize(100).execute().actionGet()
    println("-----Search hit total data:" + response.getHits.getTotalHits.toString)
    do {
      val starttime = System.currentTimeMillis()
      for (rs <- response.getHits.getHits) {
        searchHBase(rs.getSource.get("resultId").toString)
        searchRedis(TASK, rs.getSource.get("taskId").toString)
        num+=1
      }
      println(num)
      println("------------------------------Search Spend Time:" + (System.currentTimeMillis() - starttime))
      response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
    } while (response.getHits.getHits.length != 0)
  }

  def searchRedis(table: String, id: String): HashMap[String, String] = {
    var map: HashMap[String, String] = null
    if (redis.hexists(table, id)) {
      val result = redis.hget(table, id)
      map = mapper.readValue(result, new HashMap[String, String].getClass)
    }
    map

  }

  def initRedis: JedisCluster = {
    val jedisClusterNodes = new HashSet[HostAndPort]()
    //在添加集群节点的时候只需要添加一个，其余同一集群的J节点会被自动加入
    jedisClusterNodes.add(new HostAndPort("172.20.31.4", 6380))
    val jc: JedisCluster = new JedisCluster(jedisClusterNodes)
    jc
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
    //    println(result.toString)
  }

  def searchCarnumber(start: String, end: String): Unit = {
    val stype = start.split(" ")(0).split("\\-")(1).toInt
    val etype = end.split(" ")(0).split("\\-")(1).toInt
    val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes("result")
    val qu = "{\n" +
      "    \"range\": {\n" +
      "      \"ResultTime\": {\n" +
      "        \"gte\": \"" + start + "\",\n" +
      "        \"lte\": \"" + end + "\"\n" +
      "      }\n" +
      "    }\n" +
      "  }"
    var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu)).execute().actionGet()
    println("-----Search hit total data:" + response.getHits.getTotalHits.toString)
    val starttime = System.nanoTime()
    response.getHits.getTotalHits.toString
    println("------------------------------Search Spend Time:" + (System.nanoTime() - starttime) / 1000)
  }

  def search4(start: String): Unit = {
    val simplehms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now = simplehms.format(new Date(System.currentTimeMillis()))
    val stype = start.split(" ")(0).split("\\-")(1).toInt
    val etype = now.split(" ")(0).split("\\-")(1).toInt
    var num = 0
    val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes("result")
    val qu = "{\n" +
      "    \"range\": {\n" +
      "      \"ResultTime\": {\n" +
      "        \"gte\": \"" + start + "\",\n" +
      "        \"lte\": \"" + now + "\"\n" +
      "      }\n" +
      "    }\n" +
      "  }"
    var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu)).setSize(100)
      .setScroll(new TimeValue(60000)).execute().actionGet()
    println("-----Search hit total data:" + response.getHits.getTotalHits.toString)
    do {
      val starttime = System.currentTimeMillis()
      for (rs <- response.getHits.getHits) {
        searchRedis(TASK, rs.getSource.get("taskId").toString)
        searchRedis(CAMERA, rs.getSource.get("cameraId").toString)
        searchRedis(DATASOURCE, rs.getSource.get("dataSourceId").toString)
      }
      println("------------------------------Search Spend Time:" + (System.currentTimeMillis() - starttime))
      response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
    } while (response.getHits.getHits.length != 0)
  }
}

object Search {
  def main(args: Array[String]): Unit = {
    val sbe = new Search
    sbe.searchElasticHBase("2017-04-10 15:37:02", "2017-04-20 15:37:02", "白", "大众")
    //    sbe.searchCarnumber("2017-02-20 15:30:14", "2017-06-20 15:30:14")
    //        sbe.search4("2017-02-20 15:30:14")

  }
}