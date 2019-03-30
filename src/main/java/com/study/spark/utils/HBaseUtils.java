package com.study.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author: HuangSuhai
 * @Date: 2019/3/22 19:25
 * @Version 1.0
 */
public class HBaseUtils {
    Configuration configration = null;
    /**
     * 私有构造方法
     */
    private HBaseUtils(){
        configration = HBaseConfiguration.create();
        configration.set("hbase.zookeeper.quorum","s201:2181");
        configration.set("hbase.rootdir","hdfs://s201/hbase");
    }
    private static HBaseUtils instance = null;
    public static synchronized HBaseUtils getInstance(){
        if(null == instance){
            instance = new HBaseUtils();
        }
        return instance;
    }
    /**
     * 根据表名获取到 Htable 实例
     */
    public HTable getTable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }
    /**
     * 添加一条记录到 Hbase 表 70 30 128 32 核 200T 8000
     * @param tableName Hbase 表名
     * @param rowkey Hbase 表的 rowkey * @param cf Hbase 表的 columnfamily * @param column Hbase 表的列
     * @param value 写入 Hbase 表的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value){
        try {
            Connection conn = ConnectionFactory.createConnection(configration);
            TableName tname = TableName.valueOf(tableName);
            Table table = conn.getTable(tname);
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        String tableName = "category_clickcount";
        String rowkey = "20271111_88";
        String cf="info";
        String column ="click_count";
        String value = "2";
        HBaseUtils.getInstance().put(tableName,rowkey,cf,column,value);
    }
}
