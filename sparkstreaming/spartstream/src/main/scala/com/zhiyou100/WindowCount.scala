package com.zhiyou100

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

object WindowCount {
  val conf=new SparkConf().setMaster("local[*]").setAppName("window")
  val ssc=new StreamingContext(conf,Duration.apply(3000))

  def  wordCountWindow()={
    val dstream=ssc.socketTextStream("master",9999)
    val pairDstream=dstream.flatMap(x=>x.split("\\s+")).map(x=>(x,1))
    //开窗
    val result=pairDstream.reduceByKeyAndWindow((v1,v2)=>v1+v2,Minutes(1))
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
  def  wordCountSlider()={
    val dstream=ssc.socketTextStream("master",9999)
    val pairDstream=dstream.flatMap(_.split("\\s+")).map((_,1))
    val result=pairDstream.reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2,Minutes(1),Duration.apply(9000))
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    wordCountSlider()
  }

}
