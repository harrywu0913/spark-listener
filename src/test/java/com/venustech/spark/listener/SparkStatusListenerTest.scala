package com.venustech.spark.listener

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext

object SparkStatusListenerTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("app")
      .setMaster("local[4]")
      .set("spark.extraListeners", "com.venustech.spark.listener.SparkStatusListener")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val path = this.getClass.getClassLoader.getResource("netflow.log").getPath
    val netflowLog = sc.textFile(path)
    netflowLog.foreach(line => {
      println(line)
    })
  }
}
