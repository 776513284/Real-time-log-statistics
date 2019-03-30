package com.study.spark.dao

import com.study.spark.domain.CategaryClickCount
import com.study.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


/**
  * @author: HuangSuhai 
  * @Date: 2019/3/23 15:40
  * @Version 1.0
  */
object CategaryClickCountDAO {
  val tableName= "category_clickcount"
  val cf = "info"
  val qualifer = "click_count"
  /**
    * 保存数据到 HBase */
  def save(list:ListBuffer[CategaryClickCount]): Unit ={
    val table = HBaseUtils.getInstance().getTable(tableName);
    for(els <- list){
      table.incrementColumnValue(Bytes.toBytes(els.day_categaryId),
        Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.click_count)
    }
  }
  /**
    * 根据 rowkey查询值
    */
  def count(day_categary:String):Long ={
    val talbe = HBaseUtils.getInstance().getTable(tableName);
    val get = new Get(Bytes.toBytes(day_categary))
    val value =talbe.get(get).getValue(cf.getBytes(),qualifer.getBytes())
    if(value == null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }
  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CategaryClickCount]
    list.append(CategaryClickCount("20190324_1",100))
    save(list)
    print(count("20190323_2"))
  }
}
