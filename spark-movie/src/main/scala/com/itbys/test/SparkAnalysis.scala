package com.itbys.test

import com.itbys.test.util.HBaseUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}

/**
  * Author xx
  * Date 2024/5/20
  * Desc 电影数据分析
  */
object SparkAnalysis {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")
    conf.set("spark.sql.crossJoin.enabled", "true")
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    /**
      * 读取数据
      */
    // movies
    val moviesInputDF = spark.read
      .format("csv")
      .option("header", "true")
      .load("input/movies.csv")
    moviesInputDF.createOrReplaceTempView("movies_info")

    //ratings
    val ratingsInputDF = spark.read
      .format("csv")
      .option("header", "true")
      .load("input/ratings.csv")
    ratingsInputDF.createOrReplaceTempView("ratings_info")

    moviesInputDF.show()
    ratingsInputDF.show()


    /**
      * 数据分析
      */
    // 评分能显示多少人评分 评分占比和平均分
    val resDF01 = spark.sql(
      """
        |with tmp as (
        | select count(distinct(userId)) all_user_cnt from ratings_info
        |)
        |select a.title, count(*) rate_cnt, round(count(*) / c.all_user_cnt, 3) avg_rate_ratio, round(avg(rating), 2) avg_rating
        |from movies_info a
        |join ratings_info b on a.movieId = b.movieId
        |join tmp c on 1 = 1
        |group by a.title, c.all_user_cnt
      """.stripMargin)
    resDF01.show()


    /**
      * 结果到hbase
      */
    resDF01.as[(String, String, String, String)].foreach {
      x =>
        val str = x._1 + "#" + x._2 + "#" + x._3 + "#" + x._4 + "#"
        val hbaseConf: Configuration = HBaseUtils.getHBaseConfiguration("192.168.23.153", "2181", "hbase_batch_analysis")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "hbase_batch_analysis")

        val htable = HBaseUtils.getTable(hbaseConf, "hbase_batch_analysis")
        val put = new Put(Bytes.toBytes(x._1))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("all"), Bytes.toBytes(str))
        htable.put(put)
    }

  }


}
