package com.itbys.test

import com.itbys.test.util.{HBaseUtils, MyKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author  xx
  * Date  2024/5/20
  * Desc 实时热门电影：用户评分最多的top10的最新评分
  */
object StreamingAnalysisWindow {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    var topic = "rating_topic"
    val groupId = "group02"

    //读取kafka数据
    val valueDS: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, topic, groupId).map(_.value())

    //数据处理，类型转换
    val rkDS: DStream[(String, Double)] = valueDS.filter(x => x.split(",").length == 5)
      .transform { rdd =>
        val modelRDD: RDD[MovieModel] = rdd.map { x =>
          val strings: Array[String] = x.split(",")
          MovieModel(strings(0), strings(1), strings(2), strings(3).toDouble, strings(4))
        }

        val df: DataFrame = modelRDD.toDF()
        df.createOrReplaceTempView("rating_info")

        val resultDf: DataFrame = spark.sql(
          """
            |with tmp as (
            | select title, count(*) cnt
            | from rating_info
            | group by title
            | order by cnt desc
            | limit 10
            |)
            |select title, rating from (
            | select title,avg(rating) rating, max(`timestamp`) `timestamp`
            | from rating_info
            | where title in (select title from tmp)
            | group by title
            | order by `timestamp` desc
            |)
          """.stripMargin)

        resultDf.as[(String, Double)].rdd

      }

    //设置滑动窗口为600s，步长为10s
    val reduceDS: DStream[(String, Double)] = rkDS.window(Seconds(600), Seconds(10)).
      reduceByKey((first, second) => {
        if (first > second) {
          first
        } else {
          second
        }
      })
    reduceDS.print()

    //写入hbase
    rkDS.foreachRDD { rdd =>
      rdd.zipWithIndex().foreach {
        x =>
          val str = x._1._1 + "#" + x._1._2
          val hbaseConf: Configuration = HBaseUtils.getHBaseConfiguration("192.168.23.153", "2181", "hbase_real_analysis")
          hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "hbase_real_analysis")

          val htable = HBaseUtils.getTable(hbaseConf, "hbase_real_analysis")
          val put = new Put(Bytes.toBytes(x._2.toString))
          put.add(Bytes.toBytes("cf"), Bytes.toBytes("all"), Bytes.toBytes(str))
          htable.put(put)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

  case class MovieModel(title: String, userId: String, movieId: String, rating: Double, timestamp: String)


}
