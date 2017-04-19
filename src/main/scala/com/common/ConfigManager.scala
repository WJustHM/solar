package com.common

import java.sql.ResultSet
import java.util.concurrent.ConcurrentHashMap

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource
import redis.clients.jedis.{Jedis, JedisPool, JedisPubSub}

/**
  * Created by xuefei_wang on 17-4-5.
  */
object ConfigManager extends  Logging{

  var config =  new ConcurrentHashMap[String, String]

  private class Manager() extends Runnable{


    final val SQL_GET = "SELECT * FROM %s "

    var TABLE = "config"

    var datasource: MysqlDataSource = new MysqlDataSource()

    def setUpDB(datasource : MysqlDataSource): Manager ={
      this.datasource = datasource
      this
    }

    def setUpTable(table : String): Manager ={
      TABLE = table
      this
    }

    def loadConfig(): Unit ={
      val conn =  datasource.getConnection
      val statement = conn.prepareStatement(String.format(SQL_GET,TABLE))
      val response : ResultSet = statement.executeQuery()
      while (response.next()){
         val key = response.getString("key")
         val value = response.getString("value")
        config.put(key,value)
      }
      conn.close()
    }



    override def run(): Unit = {

      loadConfig()

      val jedisPool: JedisPool = new JedisPool(config.get("redis.host"), config.get("redis.port").toInt)

      lazy val subscriber: Jedis = jedisPool.getResource

      val pubSub = new JedisPubSub() {
        override def onMessage(channel: String, message: String) {
          if (message == "new") {
            loadConfig()
          }
        }

        override def onPMessage(pattern: String, channel: String, message: String) {
        }

        override def onSubscribe(channel: String, subscribedChannels: Int) {
        }

        override def onUnsubscribe(channel: String, subscribedChannels: Int) {
        }

        override def onPUnsubscribe(Pattern: String, subscribedChannels: Int) {
        }
      }


      while (true) {
        subscriber.subscribe(pubSub, "configuration")
      }

    }
  }

  def start( url: String ,userName : String ,passWord : String,datbaseName : String,table : String): Unit ={
    val datasource : MysqlDataSource =  new MysqlDataSource()
    datasource.setURL(url)
    datasource.setUser(userName)
    datasource.setPassword(passWord)
    datasource.setDatabaseName(datbaseName)
    start(datasource,table)
  }

  def start( datasource : MysqlDataSource , table : String): Unit ={
    val manager = new Manager().setUpDB(datasource).setUpTable(table)
    val subscriber = new Thread(manager)
    subscriber.setName("ConfigManager")
    subscriber.start()
  }

}
