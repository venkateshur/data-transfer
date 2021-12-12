package com.test.data.tools.utils


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.Logger

import scala.sys.process._
import scala.util.{Failure, Success, Try}

object Common {

  def initSparkSession(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).enableHiveSupport.getOrCreate()
  }

  def readTableWithQuery(query: String)(implicit spark: SparkSession): DataFrame = spark.sql(query)

  def writeCSVFile(inputDf: DataFrame, header: String, outputPath: String, delimiter: String): Unit = {
    inputDf.write.option("header", header).option("delimiter", delimiter).format("csv").mode("overwrite")
      .save(outputPath)
  }

  def writeTextFile(inputDf: DataFrame, outputPath: String): Unit = {
    inputDf.write.format("text").mode("overwrite")
      .save(outputPath)
  }

  def loadConfig(nameSpace: String = "data-transfer"): Config = {
    ConfigFactory.defaultApplication().getConfig(nameSpace)
  }

  def mergeAndDeleteFiles(mergedDir: String, path: String, fs: FileSystem, conf: Configuration, logger: Logger): Unit = {
    val outputPath = new Path(path)
    if (fs.exists(outputPath)) fs.delete(outputPath, true)

    Try(
      FileUtil.copyMerge(fs,
        new Path(mergedDir + "/" + "_temp/"),
        fs,
        outputPath,
        true,
        conf,
        null)) match {
      case Success(_) =>
        logger.info(s"Files Merged Successfully in the path : $mergedDir into file: ${path}")
      case Failure(e) =>
        logger.debug(s"""Error in merging files: $e""")
        throw e
    }
  }

  def streamZipCommands(inPath: String, outputPath: String) = {
    s"""hadoop jar contrib/streaming/hadoop-streaming-1.0.3.jar \
       |            -Dmapred.reduce.tasks=0 \
       |            -Dmapred.output.compress=true \
       |            -Dmapred.compress.map.output=true \
       |            -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
       |            -input $inPath \
       |            -output $outputPath \
       |            -mapper /bin/cat \
       |            -inputformat org.apache.hadoop.mapred.TextInputFormat \
       |            -outputformat org.apache.hadoop.mapred.TextOutputFormat""".stripMargin

  }

  def s3UploadCommands(awsAccessKey: String, awsSecretKey: String, endPoint: String, s3Path: String): String = {
    //TODO build hdfs cp command
    s"""""".stripMargin

  }

  def s3MakeDirCommands(awsAccessKey: String, awsSecretKey: String, endPoint: String, s3Bucket: String, s3Prefix: String): String = {
    //TODO build hdfs cp command
    s"""""".stripMargin

  }


  def runCmd(cmd: String, logger: Logger): Unit = {
    logger.info("Executing command: " + cmd)
    val process = Process(cmd).lineStream
    process.foreach { line => {
      logger.info(line)
      println(_)
    }
    }
  }
}
