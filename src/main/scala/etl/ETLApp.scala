package etl

import java.io.File

import etl.spark.{SparkConfig, SparkJob}
import org.slf4j.LoggerFactory

/**
  * This is the entry class for executing the job.
  *
  */
object ETLApp {

  val log = LoggerFactory.getLogger(ETLApp.getClass)

  def main(args: Array[String]) {
    log.info("ETL App started")

    val parser = new scopt.OptionParser[ScriptConfig]("scopt") {
      opt[File]('c', "job-config") required() valueName "<confFile>" action { (x, c) =>
        c.copy(jobConfFile = x)
      } text "job config location"
    }

    parser.parse(args, ScriptConfig()) map { scriptConf =>
      log.info("ETL App configuration:" + scriptConf)
      SparkJob(SparkConfig(scriptConf.jobConfFile)).start()
    }
    log.info("ETL App completed")
  }
}

case class ScriptConfig(jobConfFile: File = new File("."))


