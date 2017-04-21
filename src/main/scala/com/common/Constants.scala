package com.common

/**
  * Created by xuefei_wang on 17-2-27.
  */
object Constants extends Serializable {

  val APP_NAME = "DRIVER"

  val DURATION = 1000l

  val CHECKPOINT = "./checkpiont_trafficwrite"


  val KAFKA_SERVER = "datanode1:6667,datanode2:6667,datanode3:6667"

  val KAFKA_TOPIC = "traffic"

  val KAFKA_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer"

  val KAFKA_VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer"

  val KAFKA_KEY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer"

  val KAFKA_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer"

  val KAFKA_GROUPID = "traffic_group"

  val KAFKA_AUTO_OFFSET_RESET = "latest"

  val KAFKA_ENABLE_AUTO_COMMIT = "true"

  val KAFKA_PRODUCER_TYPE = "async"

  val KAFKA_BATCH_NUM_MESSAGES = "1000"

  val KAFKA_MAX_REQUEST_SIZE = "1000973460"


  val ALLUXIO_PATH = "/APP/SOLAR/"

  val KAFKA_ILLEGAL_TOPIC = "Illegal"

  val KAFKA_ALARM_TOPIC = ""


  val HBASE_TABLE_BASEINFO = "BaseInfo"

  val HBASE_ZOOKEEPER_QUORUM = "datanode1,datanode2,datanode3"

  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPOINT = "2181"

  val HBASE_CLIENT_WRITE_BUFFER = "12582912"

  val HBASE_CLEINT_MAX_TOTAL_TASKS = "500"

  val HBASE_CLIENT_MAX_PRESERVER_TASKS = "100"

  val HABSE_CLIENT_MAX_PERREGION_TASKS = "20"

  val HBASE_TABLE_STATISTICS = "TrafficInfo"

  val STATISTICS_FAMILY_NAME = "trafficinfo"

  val ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure"

  val HBASE_TABLE_ILLEGALINFO = ""

  val HBASE_ILLEGALINFO_COLUMNFAMILY = ""


  val ES_CLUSTER_NAME = "handge-cloud"


  //  val ES_URL = "localhost:9300"
  val ES_URL = "datanode1:9300,datanode2:9300,datanode3:9300"


  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"

  val MYSQL_USER_NAME = "root"

  val MYSQL_USER_PASSWORD = "mysql"

  val MYSQL_JDBC_URL = "jdbc:mysql://172.20.31.127:3306/solar"


  val REDIS_HOST = "DataStore"

  val REDIS_PORT = 6379


}
