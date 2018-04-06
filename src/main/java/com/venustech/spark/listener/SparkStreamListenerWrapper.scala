package com.venustech.spark.listener

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.streaming.StreamingContext

class SparkStreamListenerWrapper extends SparkListener {

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    val streamContext = StreamingContext.getActive().get
    streamContext.addStreamingListener(new ExtractStreamListener())
    println("other event ...")
  }
}
