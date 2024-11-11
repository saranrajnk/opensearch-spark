/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// defined in spark package so that I can use ThreadUtils
package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import com.codahale.metrics.Timer
import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.{getTimerContext, incrementCounter, registerGauge, stopTimer}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.util.ThreadUtils

/**
 * Spark SQL Application entrypoint
 *
 * @param args
 *   (0) sql query
 * @param args
 *   (1) opensearch index name
 * @return
 *   write sql query result to given opensearch index
 */
object FlintJob extends Logging with FlintJobExecutor {
  def main(args: Array[String]): Unit = {
    val (queryOption, resultIndexOption) = parseArgs(args)

    val conf = createSparkConf()
    val sparkSession = createSparkSession(conf)
    val applicationId =
      environmentProvider.getEnvVar("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown")
    val jobId = environmentProvider.getEnvVar("SERVERLESS_EMR_JOB_ID", "unknown")
    val warmpoolEnabled = conf.get(FlintSparkConf.WARMPOOL_ENABLED.key).toBoolean

    if (!warmpoolEnabled) {
      val jobType = conf.get("spark.flint.job.type", FlintJobType.BATCH)
      CustomLogging.logInfo(s"""Job type is: ${jobType}""")
      conf.set(FlintSparkConf.JOB_TYPE.key, jobType)

      val dataSource = conf.get("spark.flint.datasource.name", "")
      val query = queryOption.getOrElse(unescapeQuery(conf.get(FlintSparkConf.QUERY.key, "")))
      if (query.isEmpty) {
        logAndThrow(s"Query undefined for the ${jobType} job.")
      }
      val queryId = conf.get(FlintSparkConf.QUERY_ID.key, "")

      if (resultIndexOption.isEmpty) {
        logAndThrow("resultIndex is not set")
      }
      val resultIndex = resultIndexOption.get

      processStreamingJob(
        applicationId,
        jobId,
        query,
        queryId,
        dataSource,
        resultIndex,
        jobType,
        sparkSession,
        conf)
      return
    }

    handleWarmpoolJob(applicationId, jobId, sparkSession, conf)
  }

  private def handleWarmpoolJob(
      applicationId: String,
      jobId: String,
      sparkSession: SparkSession,
      conf: SparkConf): Unit = {

    val commandContext = CommandContext(
      applicationId,
      jobId,
      sparkSession,
      "", // In WP flow, FlintJob doesn't know the dataSource
      "", // In WP flow, FlintJob doesn't know the jobType
      "", // FlintJob doesn't have sessionId
      null, // FlintJob doesn't have SessionManager
      Duration.Inf, // FlintJob doesn't have queryExecutionTimeout
      -1, // FlintJob doesn't have inactivityLimitMillis
      -1, // FlintJob doesn't have queryWaitTimeMillis
      -1 // FlintJob doesn't have queryLoopExecutionFrequency
    )

    val statementExecutionManager = instantiateStatementExecutionManager(commandContext)
    statementExecutionManager.getNextStatement() match {
      case Some(flintStatement) =>
        logInfo(s"flintStatement received: ${flintStatement}")
        val jobType = flintStatement.context.get("jobType").toString
        val dataSource = flintStatement.context.get("dataSource").toString
        val resultIndex = flintStatement.context.get("resultIndex").toString
        val queryId = flintStatement.queryId
        val query = flintStatement.query

        CustomLogging.logInfo(s"""Job type is: ${jobType}""")
        conf.set(FlintSparkConf.JOB_TYPE.key, jobType)
        conf.set(FlintSparkConf.DATA_SOURCE_NAME.key, dataSource)

        if (jobType.equalsIgnoreCase(FlintJobType.STREAMING)) {
          processStreamingJob(
            applicationId,
            jobId,
            query,
            queryId,
            dataSource,
            resultIndex,
            jobType,
            sparkSession,
            conf)
        } else {
          handleInteractiveJob(
            sparkSession,
            commandContext,
            flintStatement,
            statementExecutionManager,
            applicationId,
            jobId,
            dataSource)
        }
    }
  }

  private def handleInteractiveJob(
      sparkSession: SparkSession,
      commandContext: CommandContext,
      flintStatement: FlintStatement,
      statementExecutionManager: StatementExecutionManager,
      applicationId: String,
      jobId: String,
      dataSource: String): Unit = {
    val statementRunningCount = new AtomicInteger(0)
    implicit val ec: ExecutionContext = ExecutionContext.global
    var dataToWrite: Option[DataFrame] = None
    val startTime: Long = currentTimeProvider.currentEpochMillis()
    val queryResultWriter = instantiateQueryResultWriter(sparkSession, commandContext)

    registerGauge(MetricConstants.STATEMENT_RUNNING_METRIC, statementRunningCount)
    flintStatement.running()
    statementExecutionManager.updateStatement(flintStatement)
    statementRunningCount.incrementAndGet()
    val statementTimerContext = getTimerContext(MetricConstants.STATEMENT_PROCESSING_TIME_METRIC)

    val futurePrepareQueryExecution = Future {
      statementExecutionManager.prepareStatementExecution()
    }

    try {
      val df = statementExecutionManager.executeStatement(flintStatement)
      dataToWrite = Some(
        ThreadUtils.awaitResult(futurePrepareQueryExecution, Duration(1, MINUTES)) match {
          case Right(_) => queryResultWriter.processDataFrame(df, flintStatement, startTime)
          case Left(error) =>
            handleCommandFailureAndGetFailedData(
              applicationId,
              jobId,
              sparkSession,
              dataSource,
              error,
              flintStatement,
              "",
              startTime)
        })
    } catch {
      case e: TimeoutException =>
        val error = s"Query execution preparation timed out"
        CustomLogging.logError(error, e)
        dataToWrite = Some(
          handleCommandTimeout(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            error,
            flintStatement,
            "",
            startTime))
      case NonFatal(e) =>
        val error = s"An unexpected error occurred: ${e.getMessage}"
        CustomLogging.logError(error, e)
        dataToWrite = Some(
          handleCommandFailureAndGetFailedData(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            error,
            flintStatement,
            "",
            startTime))
    } finally {
      try {
        dataToWrite.foreach(df => queryResultWriter.writeDataFrame(df, flintStatement))
        if (flintStatement.isRunning || flintStatement.isWaiting) {
          flintStatement.complete()
        }
      } catch {
        case e: Exception =>
          val error = s"""Fail to write result of ${flintStatement}, cause: ${e.getMessage}"""
          CustomLogging.logError(error, e)
          flintStatement.fail()
      } finally {
        statementExecutionManager.updateStatement(flintStatement)
        recordStatementStateChange(statementRunningCount, flintStatement, statementTimerContext)
      }
      statementExecutionManager.terminateStatementExecution()
    }
    sparkSession.stop()

    // Check for non-daemon threads that may prevent the driver from shutting down.
    // Non-daemon threads other than the main thread indicate that the driver is still processing tasks,
    // which may be due to unresolved bugs in dependencies or threads not being properly shut down.
    if (terminateJVM && threadPoolFactory.hasNonDaemonThreadsOtherThanMain) {
      logInfo("A non-daemon thread in the driver is seen.")
      // Exit the JVM to prevent resource leaks and potential emr-s job hung.
      // A zero status code is used for a graceful shutdown without indicating an error.
      // If exiting with non-zero status, emr-s job will fail.
      // This is a part of the fault tolerance mechanism to handle such scenarios gracefully
      System.exit(0)
    }
  }

  private def processStreamingJob(
      applicationId: String,
      jobId: String,
      query: String,
      queryId: String,
      dataSource: String,
      resultIndex: String,
      jobType: String,
      sparkSession: SparkSession,
      conf: SparkConf): Unit = {
    // https://github.com/opensearch-project/opensearch-spark/issues/138
    /*
     * To execute queries such as `CREATE SKIPPING INDEX ON my_glue1.default.http_logs_plain (`@timestamp` VALUE_SET) WITH (auto_refresh = true)`,
     * it's necessary to set `spark.sql.defaultCatalog=my_glue1`. This is because AWS Glue uses a single database (default) and table (http_logs_plain),
     * and we need to configure Spark to recognize `my_glue1` as a reference to AWS Glue's database and table.
     * By doing this, we effectively map `my_glue1` to AWS Glue, allowing Spark to resolve the database and table names correctly.
     * Without this setup, Spark would not recognize names in the format `my_glue1.default`.
     */
    conf.set("spark.sql.defaultCatalog", dataSource)
    configDYNMaxExecutors(conf, jobType)

    val streamingRunningCount = new AtomicInteger(0)
    val jobOperator =
      JobOperator(
        applicationId,
        jobId,
        sparkSession,
        query,
        queryId,
        dataSource,
        resultIndex,
        jobType,
        streamingRunningCount)
    registerGauge(MetricConstants.STREAMING_RUNNING_METRIC, streamingRunningCount)
    jobOperator.start()
  }

  private def instantiateStatementExecutionManager(
      commandContext: CommandContext): StatementExecutionManager = {
    import commandContext._
    instantiate(
      new StatementExecutionManagerImpl(commandContext),
      spark.conf.get(FlintSparkConf.CUSTOM_STATEMENT_MANAGER.key, ""),
      spark,
      "dummySessionId")
  }

  private def handleCommandTimeout(
      applicationId: String,
      jobId: String,
      spark: SparkSession,
      dataSource: String,
      error: String,
      flintStatement: FlintStatement,
      sessionId: String,
      startTime: Long) = {
    spark.sparkContext.cancelJobGroup(flintStatement.queryId)
    flintStatement.timeout()
    flintStatement.error = Some(error)
    super.constructErrorDF(
      applicationId,
      jobId,
      spark,
      dataSource,
      flintStatement.state,
      error,
      flintStatement.queryId,
      flintStatement.query,
      sessionId,
      startTime)
  }

  def handleCommandFailureAndGetFailedData(
      applicationId: String,
      jobId: String,
      spark: SparkSession,
      dataSource: String,
      error: String,
      flintStatement: FlintStatement,
      sessionId: String,
      startTime: Long): DataFrame = {
    flintStatement.fail()
    flintStatement.error = Some(error)
    super.constructErrorDF(
      applicationId,
      jobId,
      spark,
      dataSource,
      flintStatement.state,
      error,
      flintStatement.queryId,
      flintStatement.query,
      sessionId,
      startTime)
  }

  private def recordStatementStateChange(
      statementRunningCount: AtomicInteger,
      flintStatement: FlintStatement,
      statementTimerContext: Timer.Context): Unit = {
    stopTimer(statementTimerContext)
    if (statementRunningCount.get() > 0) {
      statementRunningCount.decrementAndGet()
    }
    if (flintStatement.isComplete) {
      incrementCounter(MetricConstants.STATEMENT_SUCCESS_METRIC)
    } else if (flintStatement.isFailed) {
      incrementCounter(MetricConstants.STATEMENT_FAILED_METRIC)
    }
  }

  private def instantiateQueryResultWriter(
      spark: SparkSession,
      commandContext: CommandContext): QueryResultWriter = {
    instantiate(
      new QueryResultWriterImpl(commandContext),
      spark.conf.get(FlintSparkConf.CUSTOM_QUERY_RESULT_WRITER.key, ""))
  }
}
