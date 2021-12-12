package com.test.data.tools


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.test.data.tools.utils.Common
import com.test.data.tools.utils.Common.{loadConfig, writeCSVFile, writeTextFile}
import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


case class TableMeta(group: String,
                     tableName: String,
                     query: String,
                     format: String,
                     outputDir: String,
                     outputFileName: String,
                     delimiter: String = ",")

case class AWSConfig(awsAccessKey: String, awsSecretKey: String, awsEndPoint: String,
                     s3HistoryBucket: String, s3Bucket: String, s3HistoryPrefix: String, s3Prefix: String)

object DataTransferApp extends App {

  private val logger = LoggerFactory.getLogger("DataTransfer")

  implicit val spark: SparkSession = Common.initSparkSession("Data Transfer")

  val currentDay = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))

  Try {
    import spark.implicits._
    val config = loadConfig("data-transfer")
    val tables = buildTables(config)
    val awsConfig = AWSConfig(config.getString("aws.access-key"),
      config.getString("aws.secret-key"),
      config.getString("aws.end-point"), config.getString("aws.s3-history-bucket"),
      config.getString("aws.s3-bucket"), "", currentDay)

    val outputBasePath = s"${config.getString("output-path")}/processed_time=$currentDay/"
    val listOfZippedFiles = tables.map { tableMeta => {
      val queryResult = Common.readTableWithQuery(tableMeta.query)

      val outputDir = outputBasePath + s"${tableMeta.tableName.replace(".", "_")}/"
      val tempOutputDir = outputBasePath + s"${tableMeta.tableName.replace(".", "_")}/_temp/"

      tableMeta.format match {
        case "text" =>
          writeTextFile(queryResult.select(concat_ws(tableMeta.delimiter, queryResult.columns.map(col): _*)),
            outputBasePath + s"${tableMeta.tableName.replace(".", "_")}")
        case "csv" =>
          val headerDf = queryResult.columns.toSeq.toDF().toDF(queryResult.columns: _*)
          writeCSVFile(headerDf.union(queryResult), "false", tempOutputDir)

      }
      val fileName = tableMeta.outputFileName.replace("DATE", currentDay)
      val outPath = outputDir + fileName
      Common.mergeAndDeleteFiles(
        tempOutputDir,
        outPath,
        FileSystem.get(spark.sparkContext.hadoopConfiguration),
        spark.sparkContext.hadoopConfiguration, logger)
      (outputDir, fileName)
    }
    }

    listOfZippedFiles.foreach { case (outDir, zipFile) => {
      val streamCommand = Common.streamZipCommands(zipFile, s"$outDir/$zipFile.zip")
      logger.info("Stream command: " + streamCommand)
      Common.runCmd(streamCommand, logger)
      val s3HistoryUploadCommand = Common.s3UploadCommands(awsConfig.awsAccessKey,
        awsConfig.awsSecretKey,
        awsConfig.awsEndPoint, awsConfig.s3HistoryBucket)

      logger.info("s3 history upload command: " + s3HistoryUploadCommand)

      val s3UploadCommand = Common.s3UploadCommands(awsConfig.awsAccessKey,
        awsConfig.awsSecretKey,
        awsConfig.awsEndPoint, awsConfig.s3HistoryBucket + "/" + awsConfig.s3Prefix + "/")

      logger.info("s3 upload command: " + s3UploadCommand)

      Common.runCmd(s3HistoryUploadCommand, logger)
      Common.runCmd(s3UploadCommand, logger)

    }
    }
  } match {
    case Success(_) =>
      logger.info("Data Transfer Execution Successful")
      spark.stop()
    case Failure(exception) =>
      spark.stop()
      logger.error("Data Transfer Execution with error: " + exception.getLocalizedMessage)
      throw exception
  }

  private def buildTables(config: Config) = {
    import collection.JavaConverters._
    config.getConfigList("tables").asScala.toList.map(conf => {
      val group = conf.getString("group")
      val tableName = conf.getString("name")
      val query = conf.getString("query").stripMargin
      val format = conf.getString("format")
      val outputDir = conf.getString("output-dir")
      val outputFileName = conf.getString("output-file-name")
      val delimiter = conf.getString("delimiter")
      TableMeta(group, tableName, query, format, outputDir, outputFileName, delimiter)
    })
  }
}

