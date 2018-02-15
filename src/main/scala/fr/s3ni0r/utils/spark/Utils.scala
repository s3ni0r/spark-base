package fr.s3ni0r.utils.spark

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait Utils {

  def getCurrentDate() = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmX")
    .withZone(ZoneOffset.UTC)
    .format(Instant.now())

  def withSparkContexts(appName: String = s"Job_${getCurrentDate()}")(block: SparkContext ⇒ Any) = {
    val conf = new SparkConf().setAppName(s"$appName")
    val sparkContext = new SparkContext(conf)
    try {
      block.apply(sparkContext)
    } finally {
      sparkContext.stop()
    }
  }

  def withSparkHiveContexts(appName: String = s"Job_${getCurrentDate()}")(block: (SparkContext, HiveContext) ⇒ Any) = {
    val conf = new SparkConf().setAppName(s"$appName")
    val sparkContext = new SparkContext(conf)
    val hiveContext = new HiveContext(sparkContext)
    try {
      block.apply(sparkContext, hiveContext)
    } finally {
      sparkContext.stop()
    }
  }

  def withSparkSqlContexts(appName: String = s"Job_${getCurrentDate()}")(block: (SparkContext, SQLContext) ⇒ Any) = {
    val conf = new SparkConf().setAppName(s"$appName")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    try {
      block.apply(sparkContext, sqlContext)
    } finally {
      sparkContext.stop()
    }
  }

  def withSparkSqlHiveContexts(appName: String = s"Job_${getCurrentDate()}", isLocal: Boolean = false)
                              (block: (SparkContext, SQLContext, HiveContext) ⇒ Any) = {
    val conf = isLocal match {
      case true ⇒ new SparkConf().setAppName(s"$appName").setMaster("local[*]")
      case _ ⇒ new SparkConf().setAppName(s"$appName")
    }

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val hiveContext = new HiveContext(sparkContext)
    try {
      block.apply(sparkContext, sqlContext, hiveContext)
    } finally {
      sparkContext.stop()
    }
  }
}
