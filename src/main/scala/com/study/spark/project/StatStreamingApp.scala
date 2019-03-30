package com.study.spark.project

import com.study.spark.dao.{CategaryClickCountDAO, CategarySearchClickCountDAO}
import com.study.spark.domain.{CategaryClickCount, CategarySearchClickCount, ClickLog}
import com.study.spark.project.util.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer

/**
  * @author: HuangSuhai 
  * @Date: 2019/3/22 13:30
  * @Version 1.0
  */
object StatStreamingApp {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[*]", "StatStreamingApp", Seconds(5))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "s202:9092,s203:9092,s204:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = List("flumeTopic").toSet
    val logs = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value())

    //156.187.29.132	2017-11-20 00:39:26	"GET www/2 HTTP/1.0"	-	200
    var cleanLog = logs.map(line=>{
      var infos = line.split("\t")
      var url = infos(2).split(" ")(1)
      var categaryId = 0
      if(url.startsWith("www")){
        categaryId = url.split("/")(1).toInt
      }
      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),categaryId,infos(4).toInt,infos(3))
    }).filter(log=>log.categaryId!=0)

    cleanLog.print()
    //每个类别的每天的点击量 (day_categaryId,1)
    cleanLog.map(log=>{
      (log.time.substring(0,8)+log.categaryId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition( partitions=>{
        val list = new ListBuffer[CategaryClickCount]
        partitions.foreach(pair=>{
          list.append(CategaryClickCount(pair._1,pair._2))
        })
        CategaryClickCountDAO.save(list)
      })
    })
    //每个栏目下面从渠道过来的流量20171122_www.baidu.com_1 100 20171122_2（渠道）_1（类别） 100
    //categary_search_count   create "categary_search_count","info"
    //124.30.187.10	2017-11-20 00:39:26	"GET www/6 HTTP/1.0"
    // 	https:/www.sogou.com/web?qu=我的体育老师	302
    cleanLog.map(log=>{
      val referer = log.referer.replace("//","/")
      val splits = referer.split("/")
      var host = ""
      if(splits.length > 2){
        host = splits(1)
      }
      (host,log.categaryId,log.time)
    }).filter(_._1!="").map(x =>{
      (x._3.substring(0,8)+"_"+x._1+"_"+x._2,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecods=>{
        val list = new ListBuffer[CategarySearchClickCount]
        partitionRecods.foreach(pair=>{
          list.append(CategarySearchClickCount(pair._1,pair._2))
        })
        CategarySearchClickCountDAO.save(list)
      })
    })

    ssc.start();
    ssc.awaitTermination();
  }
}
