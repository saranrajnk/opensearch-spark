/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// defined in spark package so that I can use ThreadUtils
package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.Duration

import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.registerGauge

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf

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
        logAndThrow(s"flintStatement received: ${flintStatement}")
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
          // TODO: Handle interactive queries in WP mode
        }
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
}
