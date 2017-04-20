package com.common

import java.net.InetAddress
import java.sql.{Connection => MysqlConnection}
import java.util
import java.util.Properties

import common.PoolConfig
import common.es.EsConnectionPool
import common.hbase.HbaseConnectionPool
import common.jdbc.JdbcConnectionPool
import common.kafka.KafkaConnectionPool
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection => HbaseConnection}
import org.apache.kafka.clients.producer.Producer
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by xuefei_wang on 17-3-8.
  */
trait Pools extends Logging with Serializable{

  private var hbaseConnectionPool : HbaseConnectionPool = _

  private var esConnectionPool: EsConnectionPool = _

  private var jdbcConnectionPool : JdbcConnectionPool = _

  private var kafkaConnectionPool : KafkaConnectionPool = _

  private var redisPool : JedisPool = _

  private def getPoolConfig: PoolConfig = {
    val poolConfig: PoolConfig = new PoolConfig
    poolConfig.setMaxTotal(100)//限制的连接数
    poolConfig.setMaxIdle(100)
    poolConfig.setMaxWaitMillis(1000000)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestOnCreate(true)
    poolConfig
  }

  private def  initHbase : HbaseConnectionPool = {
    println("===================> init Hbase ")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",Constants.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort",Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPOINT)
    conf.set("hbase.client.write.buffer",Constants.HBASE_CLIENT_WRITE_BUFFER)
    conf.set("hbase.client.max.total.tasks",Constants.HBASE_CLEINT_MAX_TOTAL_TASKS)
    conf.set("hbase.client.max.perserver.tasks",Constants.HBASE_CLIENT_MAX_PRESERVER_TASKS)
    conf.set("hbase.client.max.perregion.tasks",Constants.HABSE_CLIENT_MAX_PERREGION_TASKS)
    conf.set("zookeeper.znode.parent",Constants.ZOOKEEPER_ZNODE_PARENT)
    val hbasePool = new HbaseConnectionPool(getPoolConfig,conf)
    hbasePool
  }

  private def initEs : EsConnectionPool = {
    println("===================> init ES ")
    val  settings = Settings.builder()
    settings.put("cluster.name",Constants.ES_CLUSTER_NAME)

    val address: util.Collection[InetSocketTransportAddress] = new util.LinkedList[InetSocketTransportAddress]()
    Constants.ES_URL.split(",").foreach( s =>{
      val hp = s.split(":")
      logInfo("  ES " + hp(0) + "   " +hp(1))
      address.add(new InetSocketTransportAddress(InetAddress.getByName(hp(0)), hp(1).toInt))
    })

    val esPool = new EsConnectionPool(getPoolConfig,settings.build(),address)
    esPool
  }

  private def initJDBC : JdbcConnectionPool = {
    val mysqlPool = new JdbcConnectionPool(getPoolConfig,
      Constants.MYSQL_DRIVER,
      Constants.MYSQL_JDBC_URL,
      Constants.MYSQL_USER_NAME,
      Constants.MYSQL_USER_PASSWORD)
    mysqlPool
  }

  private def  initKafka : KafkaConnectionPool = {
    val kafkaConfig = new Properties()
    kafkaConfig.setProperty("bootstrap.servers",Constants.KAFKA_SERVER)
    kafkaConfig.setProperty("producer.type",Constants.KAFKA_PRODUCER_TYPE)
    kafkaConfig.setProperty("key.serializer",Constants.KAFKA_KEY_SERIALIZER)
    kafkaConfig.setProperty("value.serializer",Constants.KAFKA_VALUE_SERIALIZER)
    kafkaConfig.setProperty("batch.num.messages",Constants.KAFKA_BATCH_NUM_MESSAGES)
    kafkaConfig.setProperty("max.request.size",Constants.KAFKA_MAX_REQUEST_SIZE)
    kafkaConfig.setProperty("enable.auto.commit",Constants.KAFKA_ENABLE_AUTO_COMMIT)
    kafkaConfig.setProperty("auto.offset.reset",Constants.KAFKA_AUTO_OFFSET_RESET)
    val kafkaPool = new KafkaConnectionPool(getPoolConfig,kafkaConfig)
    kafkaPool
  }

  private def initRedisPool : JedisPool = {
    val pools = new JedisPool(getPoolConfig,Constants.REDIS_HOST,Constants.REDIS_PORT)
    pools
  }

  def getKafkaConn : Producer[Array[Byte], Array[Byte]]={
    synchronized{
      if (kafkaConnectionPool == null || kafkaConnectionPool.isClosed){
        kafkaConnectionPool = initKafka
      }
    }
    kafkaConnectionPool.getConnection
  }
  def returnKafkaConn(producer : Producer[Array[Byte], Array[Byte]]): Unit ={
    synchronized{
      kafkaConnectionPool.returnConnection(producer)
    }
  }

  def getMysqlConn : MysqlConnection = {
    synchronized{
      if (jdbcConnectionPool == null || jdbcConnectionPool.isClosed){
        jdbcConnectionPool = initJDBC
      }
    }
    jdbcConnectionPool.getConnection
  }
  def returnMysqlConn(conn : MysqlConnection): Unit ={
    synchronized{
      jdbcConnectionPool.returnConnection(conn)
    }
  }

  def getHbaseConn : HbaseConnection ={
    synchronized{
      if (hbaseConnectionPool == null || hbaseConnectionPool.isClosed){
        hbaseConnectionPool = initHbase
      }
    }
    hbaseConnectionPool.getConnection
  }
  def returnHbaseConn(conn : HbaseConnection) : Unit={
    synchronized{
      hbaseConnectionPool.returnConnection(conn)
    }
  }

  def getEsClient : TransportClient = {
    synchronized{
      if (esConnectionPool == null || esConnectionPool.isClosed){
        esConnectionPool = initEs
      }
    }
    esConnectionPool.getConnection
  }
  def returnEsConn(client : TransportClient) : Unit={
    synchronized{
      esConnectionPool.returnConnection(client)
    }
  }

  def getRedisConn : Jedis={
    synchronized{
      if (redisPool == null || redisPool.isClosed){
        redisPool = initRedisPool
      }
    }
    redisPool.getResource
  }
  def returnRedisConn(redis:Jedis): Unit ={
    synchronized{
      redisPool.returnResource(redis)
    }
  }
}
