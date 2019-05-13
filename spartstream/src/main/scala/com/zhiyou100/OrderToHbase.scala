package com.zhiyou100

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderToHbase {
  val conf=new SparkConf().setAppName("tohbase").setMaster("local[*]")
  val ssc=new StreamingContext(conf,Seconds(10))
  val hconfig=HBaseConfiguration.create()
  val connect=ConnectionFactory.createConnection(hconfig)
  val table=connect.getTable(TableName.valueOf("my_order"))


  def  ordertohbase()={
    val dstream=ssc.socketTextStream("master",9999)
    val result=dstream.map(x=>{
      val info=x.split("\\s+")
      info match {
        case Array(order_id,product_id,account_num,amount_num) => (product_id,(account_num.toInt,amount_num.toInt))
        case _ =>null
      }
    }).filter(x=>x!=null).reduceByKey((x1,x2)=>(x1._1+x2._1,x1._2+x2._2))
    result.foreachRDD(x=>{
      x.foreachPartition(i=>{

        for (x<-i){
         // val put=new Put(Bytes.toBytes(x._1))
          val get=new Get(Bytes.toBytes(x._1))

          val list=List(1,2,3)
          list.aggregate(1)((x1,x2)=>x1+x2,(a1,a2)=>a1+a2)

          val result=table.get(get)
          var account_num=x._2._1
          var amount_num=x._2._2
          if (result.advance()){
            val account_numvalue=result.getValue("i".getBytes(),"account_num".getBytes())
            val amount_numvalue=result.getValue("i".getBytes(),"amount_num".getBytes())
            println(s"account_numvalue=${account_numvalue} amount_numvalue=${amount_numvalue}")
            account_num=account_num+Bytes.toInt(account_numvalue)
            amount_num=amount_num+Bytes.toInt(amount_numvalue)
          }
             val put=new Put(Bytes.toBytes(x._1))
            put.addColumn("i".getBytes(),"account_num".getBytes(),Bytes.toBytes(account_num))
            put.addColumn("i".getBytes(),"amount_num".getBytes(),Bytes.toBytes(amount_num))
            table.put(put)
          }


      })
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    ordertohbase()
  }

}
