package com.traffic.search

import java.text.SimpleDateFormat
import java.util._
import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

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
  //连接已建立
  val redis = initRedis
  val tablename = TableName.valueOf("Result")
  val gettable = conn.getTable(tablename)
  val mapper = new ObjectMapper()
  val TASK = "TASK"
  val CAMERA = "CAMERA"
  val DATASOURCE = "DATASOURCE"
  var num = 0;

  //查询一
  def searchElasticHBase(vehicleBrand: String, PlateColor: String, Direction: String, tag: String
                         , paper: String, sun: String, drop: String
                         , secondBelt: String, crash: String, danger: String
                         , starttime: String, endtime: String
                        ): Unit = {
    //建立ES索引连接
    val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes("result")
    //ES查询Json代码
    val qu = "{\n" +
      "    \"bool\": {\n" +
      "      \"must\": [\n" +
      "        {\n" +
      "          \"term\": {\n" +
      "            \"vehicleBrand.keyword\": {\n" +
      "              \"value\": \"" + vehicleBrand + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"PlateColor.keyword\": {\n" +
      "              \"value\": \"" + PlateColor + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"Direction\": {\n" +
      "              \"value\": \"" + Direction + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"tag.keyword\": {\n" +
      "              \"value\": \"" + tag + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"paper.keyword\": {\n" +
      "              \"value\": \"" + paper + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"sun.keyword\": {\n" +
      "              \"value\": \"" + sun + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"drop.keyword\": {\n" +
      "              \"value\": \"" + drop + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"secondBelt.keyword\": {\n" +
      "              \"value\": \"" + secondBelt + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"crash.keyword\": {\n" +
      "              \"value\": \"" + crash + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"danger.keyword\": {\n" +
      "              \"value\": \"" + danger + "\"\n" +
      "            }\n" + "          }\n" + "        }\n" + "      ],\n" +
      "      \"filter\": {\n" +
      "        \"range\": {\n" +
      "          \"ResultTime\": {\n" +
      "            \"gte\": \"" + starttime + "\",\n" +
      "            \"lte\": \"" + endtime + "\"\n" +
      "          }\n" + "        }\n" + "      }\n" + "    }\n" +
      "  }"
    //执行查询语句
    var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu))
      .addSort("ResultTime", SortOrder.ASC).setScroll(new TimeValue(60000)).setSize(50).execute().actionGet()
    println("-----Search hit total data:" + response.getHits.getTotalHits.toString)
    val fuList = new util.LinkedList[Future[util.Map[String, String]]]()
    do {
      val starttime = System.currentTimeMillis()
      for (rs <- response.getHits.getHits) {
        //异步查询
        val res = Search.pool.submit(new Callable[util.Map[String, String]] {
          override def call(): util.Map[String, String] = {
            val resultHBase = searchHBase(rs.getSource.get("resultId").toString)
            val resultRdeis = searchRedis(TASK, rs.getSource.get("taskId").toString)
            joinElasticHBase(resultHBase, resultRdeis)
            resultHBase
          }
        })
        fuList.add(res)
      }
      //迭代查询结果
      val d = fuList.iterator()
      while (d.hasNext) {
        d.next().get().get("HP")
      }
      fuList.clear()
      //计算批次返回时间
      println("------------------------------Search Spend Time:" + (System.currentTimeMillis() - starttime))
      response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
    } while (response.getHits.getHits.length != 0)
  }

  //查询结果
  def joinElasticHBase(resHBase: Map[String, String], resRedis: Map[String, String]): Map[String, String] = {
    val result = new HashMap[String, String]
    result
  }

  //查询结果
  def joinRedis(resRedistask: Map[String, String], resRediscamera: Map[String, String], resRedisdataSource: Map[String, String]): Map[String, String] = {
    val result = new HashMap[String, String]
    result
  }

  //执行Redis查询
  def searchRedis(table: String, id: String): HashMap[String, String] = {
    var map: HashMap[String, String] = null
    if (redis.hexists(table, id)) {
      val result = redis.hget(table, id)
      map = mapper.readValue(result, new HashMap[String, String].getClass)
    }
    map
  }

  //建立Redis连接
  def initRedis: JedisCluster = {
    val jedisClusterNodes = new HashSet[HostAndPort]()
    //在添加集群节点的时候只需要添加一个，其余同一集群的J节点会被自动加入
    jedisClusterNodes.add(new HostAndPort("172.20.31.4", 6380))
    val jc: JedisCluster = new JedisCluster(jedisClusterNodes)
    jc
  }

  //HBase查询代码
  def searchHBase(rw: String): Map[String, String] = {
    val resultHbase = new util.HashMap[String, String]()
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
    resultHbase.put("HP", License)
    resultHbase
  }

  //查询三
  def searchCarnumber(starttime: String, endtime: String): Unit = {
    //建立ES索引连接
    val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes("result")
    //ES查询Json代码
    val qu = "{\n" +
      "    \"range\": {\n" +
      "      \"ResultTime\": {\n" +
      "        \"gte\": \"" + starttime + "\",\n" +
      "        \"lte\": \"" + endtime + "\"\n" +
      "      }\n" +
      "    }\n" +
      "  }"
    //执行查询语句
    var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu)).execute().actionGet()
    println("-----Search hit total data:" + response.getHits.getTotalHits.toString)
    val start = System.nanoTime()
    response.getHits.getTotalHits.toString
    println("------------------------------Search Spend Time:" + (System.nanoTime() - start) / 1000)
  }

  //查询四
  def searchLicense(starttime: String, province: String, regexnumber: String): Unit = {
    val simplehms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now = simplehms.format(new Date(System.currentTimeMillis()))
    var num = 0
    //建立ES索引连接
    val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes("result")
    //ES查询Json代码
    val qu = "{\n" +
      "    \"bool\": {\n" +
      "      \"must\": [\n" +
      "        {\n" +
      "          \"regexp\": {\n" +
      "            \"Plate.keyword\": {\n" +
      "              \"value\": \"" + province + ".{5}" + regexnumber + "\"\n" +
      "            }\n" +
      "          }\n" +
      "        },\n" +
      "        {\n" +
      "          \"range\": {\n" +
      "            \"ResultTime\": {\n" +
      "              \"gte\": \"" + starttime + "\",\n" +
      "              \"lte\": \"" + now + "\"\n" +
      "            }\n" +
      "          }\n" +
      "        }\n" +
      "      ]\n" +
      "    }\n" +
      "  }"
    //执行查询语句
    var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu)).addSort("ResultTime", SortOrder.ASC).setSize(100)
      .setScroll(new TimeValue(60000)).execute().actionGet()
    println("-----Search hit total data:" + response.getHits.getTotalHits.toString)
    val fuList = new util.LinkedList[Future[util.Map[String, String]]]()
    do {
      val starttime = System.currentTimeMillis()
      for (rs <- response.getHits.getHits) {
        //异步查询
        val res = Search.pool.submit(new Callable[util.Map[String, String]] {
          override def call(): util.Map[String, String] = {
            val task = searchRedis(TASK, rs.getSource.get("taskId").toString)
            val camera = searchRedis(CAMERA, rs.getSource.get("cameraId").toString)
            val dataSource = searchRedis(DATASOURCE, rs.getSource.get("dataSourceId").toString)
            joinRedis(task, camera, dataSource)
            task
          }
        })
        fuList.add(res)
      }
      println("------------------------------Search Spend Time:" + (System.currentTimeMillis() - starttime))
      response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
    } while (response.getHits.getHits.length != 0)
  }
}

object Search {
  //创建线程池
  val pool: ExecutorService = Executors.newWorkStealingPool(50)

  def main(args: Array[String]): Unit = {
    val sbe = new Search
    //        sbe.searchElasticHBase("别克", "黄", "1", "true", "false", "false", "true", "true", "true", "false", "2017-04-10 15:37:02", "2017-04-20 15:37:02")
    //    sbe.searchCarnumber("2017-04-20 15:30:14", "2017-04-25 15:30:14")
    //    sbe.searchLicense("2017-04-20 15:30:14", "粤", "2")

  }
}