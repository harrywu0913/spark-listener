package com.venustech.spark.listener

import org.apache.spark.streaming.scheduler._

class ExtractStreamListener extends StreamingListener{
  /** Called when a receiver has been started */
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    println("receiverStarted...")
  }

  /** Called when a receiver has reported an error */
  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    println("receiverError...")
  }

  /** Called when a receiver has been stopped */
  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    println("receiver stopped!")
  }

  /** Called when a batch of jobs has been submitted for processing. */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    println("batch submitted...")
  }

  /** Called when processing of a batch of jobs has started.  */
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    println("batch started")
  }

  /** Called when processing of a batch of jobs has completed. */
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    println("batch completed...")
  }

  /** Called when processing of a job of a batch has started. */
  override def onOutputOperationStarted(
                                outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    println("output operation started")
  }

  /** Called when processing of a job of a batch has completed. */
  override def onOutputOperationCompleted(
                                  outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    println("output operation completed")
  }

}
