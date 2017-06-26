package etl.spark

import java.io.{File, FileInputStream}
import java.util.Properties

import enron.{AverageLengthProcessor, EnronEmailProcessor, TopRecipientsProcessor}
import org.apache.commons.lang3.StringUtils.isNotBlank
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}


class SparkJob(sparkConfig: SparkConfig) {

  val log: Logger = LoggerFactory.getLogger(SparkJob.this.getClass)

  def start(): Unit = {
    log.info(s"Starting Spark job ${sparkConfig.jobName}")
    log.info(s"Spark Config $sparkConfig")


    val sparkSessionBuilder = SparkSession.builder
    sparkSessionBuilder.appName(sparkConfig.jobName)
    if (isNotBlank(sparkConfig.sparkMaster)) sparkSessionBuilder.master(sparkConfig.sparkMaster)
    if (isNotBlank(sparkConfig.sparkLocalDir)) sparkSessionBuilder.config("spark.local.dir", sparkConfig.sparkLocalDir)
    if (sparkConfig.prop.contains("awsAccessKey")) {
      sparkSessionBuilder.config("spark.hadoop.fs.s3.awsAccessKeyId", sparkConfig.prop.getProperty("awsAccessKey"))
      sparkSessionBuilder.config("spark.hadoop.fs.s3n.awsAccessKeyId", sparkConfig.prop.getProperty("awsAccessKey"))
      sparkSessionBuilder.config("spark.hadoop.fs.s3.awsSecretAccessKey", sparkConfig.prop.getProperty("awsSecretKey"))
      sparkSessionBuilder.config("spark.hadoop.fs.s3n.awsSecretAccessKey", sparkConfig.prop.getProperty("awsSecretKey"))

    }
    val sparkSession = sparkSessionBuilder.getOrCreate()
    log.info(s"Loaded Spark context")
    val sc = sparkSession.sparkContext
    if (sparkConfig.prop.contains("awsAccessKey")) {
      sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", sparkConfig.prop.getProperty("awsAccessKey"))
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", sparkConfig.prop.getProperty("awsSecretKey"))

    }

    val processor = sparkConfig.processor match {
      case "EnronEmailProcessor" => EnronEmailProcessor(sparkConfig.prop)
      case "AverageLengthProcessor" => AverageLengthProcessor(sparkConfig.prop)
      case "TopRecipientsProcessor" => TopRecipientsProcessor(sparkConfig.prop)
    }
    processor.run(sparkSession)


    sparkSession.close()
    log.info(s"Finished Spark job ${sparkConfig.jobName}")
  }
}

object SparkJob {
  def apply(sparkConfig: SparkConfig): SparkJob = new SparkJob(sparkConfig)
}

trait SparkProcessor {
  def run(sparkSession: SparkSession): Unit
}


case class SparkConfig(jobName: String, sparkMaster: String, sparkLocalDir: String, processor: String, prop: Properties)

object SparkConfig {
  def apply(prop: Properties): SparkConfig = {
    SparkConfig(
      prop.getProperty("jobName"),
      prop.getProperty("sparkMaster"),
      prop.getProperty("sparkLocalDir"),
      prop.getProperty("processor"),
      prop
    )
  }

  def apply(propertiesFile: File): SparkConfig = {
    val prop = new Properties()
    val in = getClass.getResourceAsStream(propertiesFile.getPath)

    prop.load(in)
    SparkConfig(prop)
  }

}