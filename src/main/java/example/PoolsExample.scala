package example

import java.util
import java.util.concurrent.atomic.AtomicLong

import com.common.Pools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Put, RetriesExhaustedWithDetailsException}
import org.elasticsearch.action.index.IndexRequest


/**
  * Created by xuefei_wang on 17-3-8.
  */
object PoolsExample  extends  Pools{

  def testHbase : Unit = {

    val listener_traffic =  new BufferedMutator.ExceptionListener(){
      override def onException(exception: RetriesExhaustedWithDetailsException, mutator: BufferedMutator): Unit = {
        println("Traffic exception "+exception.getMessage)
      }
    }

    val params_Traffic = new BufferedMutatorParams(TableName.valueOf("Traffic")).listener(listener_traffic)

    val conn = getHbaseConn
    val mutator  = conn.getBufferedMutator(params_Traffic)

    val put = new Put("test".getBytes)
    put.addColumn("MetaData".getBytes,"SBBH".getBytes,"test".getBytes)
    mutator.mutate(put)
    mutator.flush()


  }

  def testKafka : Unit = {
    val conn = getKafkaConn
    println(conn.partitionsFor("test"))

  }

  def testMysql : Unit = {

    val conn = getMysqlConn
    val response = conn.prepareStatement("show databases").executeQuery()
    while(response.next()){
      println(response.getString("Database"))
    }


  }

  def testResid : Unit = {

    val redis = getRedisConn
    println(redis.hset("test","test","testtesttesttesttest"))
  }

  def testES : Unit = {

    val esclient = getEsClient
    val esclientBuk = esclient.prepareBulk()


    val indexRequest = new IndexRequest("traffic","traffic")
    val data = new util.HashMap[String,String]()
    data.put("SBBH","SBBH")
    indexRequest.source(data)

    esclientBuk.add(indexRequest)


   val r     = esclientBuk.execute()
 println(r)

  }

  def main(args: Array[String]): Unit = {

    val timeer = System.currentTimeMillis()

    println(timeer)
    while (true){
      println(System.nanoTime())
    }


    println(String.valueOf(timeer).reverse)


  }
}
