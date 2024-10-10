/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.config.{THREAD_DUMP_COLLECTOR_DIR,
  THREAD_DUMP_COLLECTOR_OUTPUT_TYPE}
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Creates a thread dump collector thread which will call the specified collectThreadDumps
 * function at intervals of intervalMs.
 *
 * @param collectThreadDumps the thread dump collector function to call.
 * @param name               the thread name for the thread dump collector.
 * @param intervalMs         the interval between stack trace collections.
 */
private[spark] class ThreadDumpCollector(
                                          collectThreadDumps: () => Unit,
                                          name: String,
                                          intervalMs: Long) extends Logging {
  // Executor for the thread dump collector task
  private val threadDumpCollector = ThreadUtils.newDaemonSingleThreadScheduledExecutor(name)

  /** Schedules a task to collect the thread dumps */
  def start(): Unit = {
    val threadDumpCollectorTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(collectThreadDumps())
    }
    threadDumpCollector.scheduleAtFixedRate(threadDumpCollectorTask, intervalMs, intervalMs,
      TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    threadDumpCollector.shutdown()
    threadDumpCollector.awaitTermination(10, TimeUnit.SECONDS)
  }

}

private[spark] object ThreadDumpCollector extends Logging {
  def saveThreadDumps(env: SparkEnv): Unit = {
    env.conf.get(THREAD_DUMP_COLLECTOR_OUTPUT_TYPE) match {
      case "LOG" => writeThreadDumpsToLog(env)
      case "FILE" => writeThreadDumpsToFile(env)
    }
  }

  /** Print all thread dumps */
  private def writeThreadDumpsToLog(env: SparkEnv): Unit = {
    logWarning(log"Thread dumps from ${MDC(LogKeys.EXECUTOR_ID, env.executorId)}:\n" +
      log"${MDC(LogKeys.THREAD_DUMPS, Utils.getThreadDump().map(_.toString).mkString)}")
  }

  private def writeThreadDumpsToFile(env: SparkEnv): Unit = {
    val timestamp = Instant.now().getEpochSecond().toString()
    val threadDumpFileName = env.conf.getAppId + "-" + env.executorId + "-" + timestamp + ".txt"
    val collectedThreadDump = Utils.getThreadDump().map(_.toString).mkString
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(env.conf)
    val rootDir = env.conf.get(THREAD_DUMP_COLLECTOR_DIR)
    val fileSystem: FileSystem = new Path(rootDir).getFileSystem(hadoopConf)
    val threadDumpFilePermissions = new FsPermission(Integer.parseInt("770", 8).toShort)
    val dfsLogFile: Path = fileSystem.makeQualified(new Path(rootDir, threadDumpFileName))
    try {
      val outputStream = SparkHadoopUtil.createFile(fileSystem, dfsLogFile, true)
      fileSystem.setPermission(dfsLogFile, threadDumpFilePermissions)
      outputStream.write(collectedThreadDump.getBytes(StandardCharsets
        .UTF_8))
      outputStream.close()
    } catch {
      case e: Exception =>
        logError(
          log"Could not save thread dumps into file from executor ${
            MDC(LogKeys.EXECUTOR_ID,
              env.executorId)
          }", e)
    }
  }
}
