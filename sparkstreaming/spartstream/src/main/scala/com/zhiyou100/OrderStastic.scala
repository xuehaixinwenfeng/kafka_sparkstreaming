package com.zhiyou100

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

object OrderStastic {
  val conf=new SparkConf().setAppName("order").setMaster("local[*]")
  val ssc=new StreamingContext(conf,Duration.apply(10000))
  val url="jdbc:mysql://localhost:3306/test3"
  val driverClass="com.mysql.jdbc.Driver"
  val userName="root"
  val password="123456"
  Class.forName(driverClass)
  val connecionFactory=()=>{DriverManager.getConnection(url,userName,password)}
  def  orderstatic()={
    val dstream=ssc.socketTextStream("master",9999)
  //  val result=dstream.

    val result=dstream.map(x=>{
      val info = x.split("\\s+")
      info match {
        case Array(_,product_id,account_num,amount_num) => (product_id,(account_num.toInt,amount_num.toInt))
        case _=>null
      }
    }).filter(x=>x!=null)
      .reduceByKey((x1,x2)=>(x1._1+x2._1,x1._2+x2._2))

    result.foreachRDD(rdd=>rdd.foreachPartition( i=>{for(x<-i){
      val connection=connecionFactory()
      val stmt=connection.createStatement()
      val sql=s"select * from order_static where product_id=${x._1}"
      val result=stmt.executeQuery(sql)
      var account_num=x._2._1
      var amount_num=x._2._2
      var updatesql=s"insert into order_static(product_id,account_num,amount_num) values('${x._1}','${account_num}','${amount_num}')"
      if (result.next()){
        account_num += result.getInt("account_num")
        amount_num += result.getInt("amount_num")
        updatesql=s"update order_static set account_num=${account_num},amount_num=${amount_num} where product_id=${x._1}"
      }
      stmt.execute(updatesql)


    }}))
    ssc.start()
    ssc.awaitTermination()

  }
  def calcOrderStasticWithState()={
    val dstream=ssc.socketTextStream("master",9998)
    val result=dstream.map(x=>{
      val info = x.split("\\s+")
      info match {
        case Array(_,product_id,account_num,amount_num) => (product_id,(account_num.toInt,amount_num.toInt))
        case _=>null
      }
    }).filter(x=>x!=null)
  //    .reduceByKey((x1,x2)=>(x1._1+x2._1,x1._2+x2._2))
    ssc.checkpoint("file:///e:/aaa")
    val r=result.updateStateByKey((value:Seq[(Int,Int)],state:Option[(Int,Int)])=>{

        state match {
          case Some(s) => Some((s._1+(if (value.size>0) value(0)._1 else 0),s._2+(if (value.size>0) value(0)._2 else 0)))
          case None => Some(value(0))
        }

    })
    val r2=result.updateStateByKey((value:Seq[(Int,Int)],state:Option[(Int,Int)])=>{
      val res=value.aggregate((0,0))((x1,x2)=>(x1._1+x2._1,x1._2+x2._2),(c1,c2)=>(c1._1+c2._1,c1._2+c2._2))

     // value.reduce((x1,x2)=>(x1._1+x2._1,x1._2+x2._2))
      state match {
        case Some(s) => Some((s._1+(if (value.size>0) res._1 else 0),s._2+(if (value.size>0) res._2 else 0)))
        case None => Some(res)
      }

    })
    r2.print()
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    calcOrderStasticWithState()
  }

}
