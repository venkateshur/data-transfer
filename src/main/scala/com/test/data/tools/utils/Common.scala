package com.test.data.tools.utils


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import java.io.IOException

import org.slf4j.Logger

import scala.sys.process._
import scala.util.{Failure, Success, Try}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, CompressionOutputStream}

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

  def streamZipCommands(inPath: String, outputPath: String): String = {
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
    //TODO build hdfs mkdir command
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

  def copyMerge(
                 srcFS: FileSystem, srcDir: Path,
                 dstFS: FileSystem, dstFile: Path,
                 deleteSource: Boolean, conf: Configuration
               ): Boolean = {

    if (dstFS.exists(dstFile))
      throw new IOException(s"Target $dstFile already exists")

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {

      val outputFile = dstFS.create(dstFile)

      val factory = new CompressionCodecFactory(conf)
      val codec = factory.getCodecByClassName("org.apache.hadoop.io.compress.GzipCodec")
      val compressionOutputStream = codec.createOutputStream(outputFile)

      Try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile() =>
              val inputFile = srcFS.open(status.getPath())
              Try(IOUtils.copyBytes(inputFile, compressionOutputStream, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()

      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }
}
