package com.zhiyou100

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

object TcpWordCount {
  val conf=new SparkConf().setAppName("stream test").setMaster("local[*]")
  val stream=new StreamingContext(conf,Duration.apply(10000))

  def tcpwordcount()={
    val dstream = stream.socketTextStream("master",9999)
    val result=dstream.flatMap(x=>x.split("\\s+"))
                        .map(x=>(x,1)).reduceByKey(_+_)
    result.print()
    stream.start()
    stream.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    tcpwordcount()
  }

}
