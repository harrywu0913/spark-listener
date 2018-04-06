package com.venustech.spark.listener

import java.util.Date

import com.venustech.spark.dto.SparkApplication
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.status.api.v1

class SparkStatusListener(conf: SparkConf) extends SparkListener {

  val appInfo = new SparkApplication()

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println("stage Completed start ...")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println("stage submitted ...")
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    println("task start ...")
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    println("task getting result ...")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println("task end ...")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println("job start ...")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("job end ...")
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    println("environment update...")
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    println("block Manager added ...")
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    println("block manager removed ...")
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
    println("unpersist rdd ...")
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    println("application start ...")
    appInfo.setAppId(applicationStart.appId.toString)
    appInfo.setAppName(applicationStart.appName.toString)
    appInfo.setStartTime(applicationStart.time)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("application end ...")
    appInfo.setEndTime(applicationEnd.time)
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    println("executor metrics update ...")
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    println("executor added ...")
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    println("executor remove ...")
  }

//  override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {
//    println("executor blackisted ...")
//  }
//
//  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {
//    println("executor unblacklisted ...")
//  }
//
//  override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {
//    println("none blacklisted ...")
//  }
//
//  override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = {
//    println("none unblacklisted ...")
//  }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    println("block updated ...")
  }

//  override def onSpeculativeTaskSubmitted
//  (speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = {
//    println("speculative task submmited ...")
//  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    println("other event ...")
  }
}
