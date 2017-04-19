package com.service.overspeed


import java.util

import com.common.{BootStrap, Constants, Pools}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import protocol.OffenceSnapDataProtos.OffenceSnapData


/**
  * Created by xuefei_wang on 17-3-1.
  */
class OverSpeed extends BootStrap with Serializable {
  val speedLimitMap=getSpeedLimitMap
  override def setUpDstream(dstream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit = {
    val streams = dstream.map(r => OffenceSnapData.parseFrom(r.value()))
    val illegalSpeed = streams.map(r => {
      val id = r.getId //卡口编号
      val vehicleType = r.getVehicleInfo.getVehicleType //车辆类型
      val VehicleSpeed = r.getVehicleInfo.getSpeed //车辆速度
      (id, vehicleType, VehicleSpeed, r)
    })
    illegalSpeed.foreachRDD(r => {
      r.foreachPartition(r => {
        val hbaseConnection = OverSpeed.getHbaseConn
        r.foreach(r => {
          OverSpeed.all = OverSpeed.all + 1
          var standSpeed = speedLimitMap(r._1)
          if(standSpeed.contains(",")){
            standSpeed=standSpeed.split(",")(r._2)
          }
          logInfo("第" + OverSpeed.all.toString + "条数据" + "       " + "车牌号：" + r._4.getPlateInfo.getLicense + "     卡口："+
            r._1 + "  速度：" + r._3 + "-----------------  标准速度为：" + standSpeed + "===========>未超速")
          if (r._3 > standSpeed.toFloat) {
            val rowKey = r._4.getTime + "_" + r._4.getPlateInfo.getLicense + "_" + r._1
            OverSpeed.illegal = OverSpeed.illegal + 1
            val illegalRate = getOverSpeedRate(standSpeed.toString, r._3.toString) //超速比
            logInfo("第" + OverSpeed.illegal.toString + "条超速数据" + "       " + "车牌号：" +r._4.getPlateInfo.getLicense +
              "     卡口：" + r._1 + "  速度：" + r._3 + "    超速比" + illegalRate.toString())
            var map: Map[String, String] = Map()
            map += ("KKBH" -> r._1) //卡口编号
            map += ("CLBH" -> r._4.getVehicleInfo.getChanIndex.toString) //车辆编号
            map += ("FXBH" -> r._4.getDir.toString) //方向编号
            map += ("HPHM" -> r._4.getPlateInfo.getLicense) //号牌号码
            map += ("HPZL" -> r._4.getPlateInfo.getPlateType) //号牌种类
            map += ("WFDD" -> r._4.getMonitoringSiteID) //违法地点//监测点
            map += ("WFSJ" -> r._4.getTime) //违法时间
            map += ("WFXW" -> r._4.getIllegalType.toString) //违法行为
            map += ("CLSD" -> r._3.toString) //车辆速度
            map += ("CLLX" -> r._2.toString) //车辆类型
            map += ("TZTX" -> "") //特征图像
            map += ("QJTX" -> "") //全景图像
            map += ("CSB" -> illegalRate.toString) //超速比
            map += ("SFCL" -> "") //是否处理
            //插入Hbase
            writeToHBase(hbaseConnection,rowKey, map)
          }
        })
        OverSpeed.returnHbaseConn(hbaseConnection)
      })
    })
  }

  override def setAppName(name: String): Unit = {
    this.AppName = name
  }

  /**描述：根据限速与车辆速度计算超速比
    *
    * @param standard 限速
    * @param current  车辆当前速度
    * @return Float
    */
  def getOverSpeedRate(standard: String, current: String): Float = {
    val df = new java.text.DecimalFormat("#.##");
    val rate = (current.toFloat - standard.toFloat) / standard.toFloat
    df.format(rate).toFloat
  }

  /**描述：从mysql中得到卡口id与车辆速度的关系
    *
    * @return Map[String,String]
    */
   def getSpeedLimitMap(): Map[String,String] = {
    var speedLimit =""
    val mysqlConn = OverSpeed.getMysqlConn
    val rs = mysqlConn.prepareStatement("select id,speedlimit from gate").executeQuery()
    var map:Map[String,String]=Map()
    while (rs.next()) {
      speedLimit = rs.getString("speedlimit")
      map += (rs.getInt("id").toString -> speedLimit)
    }
     OverSpeed.returnMysqlConn(mysqlConn)
    map
  }

  /**描述：将超速数据插入到IllegalInfo表中
    *
    * @param conn hbase连接
    * @param row  RowKey：时间+车牌+卡口
    * @param map  列限定符与插入值的映射
    */
  def writeToHBase(conn:Connection,row: String,map:Map[String, String]):Unit = {
    val t: TableName = TableName.valueOf(Constants.HBASE_TABLE_ILLEGALINFO)
    val bufferedMutator = conn.getBufferedMutator(t)
    val puts : util.ArrayList[Put] = new util.ArrayList[Put]()
    for(d <- map){
      val put = new Put(Bytes.toBytes(row))
      put.addColumn(Bytes.toBytes(Constants.HBASE_ILLEGALINFO_COLUMNFAMILY),Bytes.toBytes(d._1),Bytes.toBytes(d._2))
      puts.add(put)
    }
    bufferedMutator.mutate(puts)
    bufferedMutator.flush()
  }
}

object OverSpeed extends Serializable with Pools{
  var all = 0
  var illegal = 0
  def main(args: Array[String]): Unit = {
    val overSpeed = new OverSpeed
    overSpeed.setAppName("OverSpeed")
    overSpeed.run()
  }
}

