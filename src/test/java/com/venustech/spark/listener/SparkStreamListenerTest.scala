package com.venustech.spark.listener

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext}

object SparkStreamListenerTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("appName")
      .setMaster("local[4]")
      .set("spark.extraListeners", "com.venustech.spark.listener.SparkStreamListenerWrapper")
      .set("spark.executor.memory", "1g")
    val ssc = new StreamingContext(conf, Duration(1000))
    val socketStreams = ssc.socketTextStream("localhost", 1314, StorageLevel.MEMORY_AND_DISK_2)
    socketStreams.foreachRDD(rdd => {
      println(rdd)
    })
    ssc.awaitTermination()
    ssc.start()
  }
}
