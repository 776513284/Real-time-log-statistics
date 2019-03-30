package com.study.spark.dao

import com.study.spark.domain.CategarySearchClickCount
import com.study.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * @author: HuangSuhai 
  * @Date: 2019/3/23 16:58
  * @Version 1.0
  */
object CategarySearchClickCountDAO {
  val tableName = "category_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"
  /**
    * 保存数据
    * @param list */
  def save(list:ListBuffer[CategarySearchClickCount]): Unit ={
    val table = HBaseUtils.getInstance().getTable(tableName)
    for(els <- list){
      table.incrementColumnValue(Bytes.toBytes(els.day_search_categary),
        Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.clickCount);
    }
  }
  def count(day_categary:String) : Long={
    val table =HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_categary))
    val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
    if(value == null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }
  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CategarySearchClickCount]
    list.append(CategarySearchClickCount("20190323_1_8",300))
    list.append(CategarySearchClickCount("20190323_2_9", 600))
    list.append(CategarySearchClickCount("20190323_2_10", 1600))
    save(list)
    print(count("20190323_2_9"))
  }
}
