package com.service.hunter

import java.io.Serializable
import java.util.concurrent.ConcurrentHashMap

import com.common.Pools
import redis.clients.jedis.{Jedis, JedisPubSub}

/**
  * Created by dalu on 17-3-7.
  */
class SubManager() extends Runnable with Serializable with Pools{
  val subscriber: Jedis = getRedisConn
  def run() {
    subscriber.subscribe(jedisPubSub, "channels")
  }

  val jedisPubSub: JedisPubSub = new JedisPubSub() {
    override def onMessage(channel: String, message: String) {
      System.out.println("get Message " + message)
      if (message == "new")
        getBlackInfo()
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

  def getBlackInfo() = {
    val mysqlConnection = getMysqlConn
    var HPHM: String = ""
    var BKXXBH: String = ""
    var BKJB: String = ""
    val result = mysqlConnection.createStatement.executeQuery("select * from PreyInfo")
    while (result.next) {
      HPHM = result.getString("HPHM")
        val blackInfo = new ConcurrentHashMap[String, String]
        BKXXBH = result.getString("BKXXBH")
        BKJB = result.getString("BKJB")
        blackInfo.put("BKXXBH", BKXXBH)
        blackInfo.put("BKJB", BKJB)
        Hunter.blackInfoMap.put(HPHM, blackInfo)
    }
    returnMysqlConn(mysqlConnection)
  }
}

