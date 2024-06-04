package com.itbys.test.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._

/**
  * Author xx
  * Date 2022/2/3
  * Desc 读写kafka
  */
object MyKafkaUtil {

  val KAFKA_SERVER = "192.168.23.153:9092"

  /**
    * =====接收消息=====
    */
  // kafka消费者配置
  val kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> KAFKA_SERVER, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall_consumer_group01",
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "earliest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
    * @desc 获取流
    * @param: ssc
    * @param: topic
    * @return: org.apache.spark.streaming.dstream.InputDStream<org.apache.kafka.clients.consumer.ConsumerRecord<java.lang.String,java.lang.String>>
    */
  def getKafkaStream(ssc: StreamingContext, topic: String) = {
    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
  }

  /**
    * @desc 获取流：使用自定义的消费者组
    * @param: ssc
    * @param: topic
    * @param: consumer
    * @return: org.apache.spark.streaming.dstream.InputDStream<org.apache.kafka.clients.consumer.ConsumerRecord<java.lang.String,java.lang.String>>
    */
  def getKafkaStream(ssc: StreamingContext, topic: String, consumer: String) = {
    kafkaParam("group.id") = consumer
    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
  }

  /**
    * @desc: 获取流：使用自定义的消费者组、偏移量
    * @param ssc      : StreamingContext
    * @param topic    : 主题
    * @param consumer : 消费组
    * @param offsets  : 偏移量
    * @return: org.apache.spark.streaming.dstream.InputDStream<org.apache.kafka.clients.consumer.ConsumerRecord<java.lang.String,java.lang.String>>
    **/
  def getKafkaStream(ssc: StreamingContext, topic: String, consumer: String, offsets: Map[TopicPartition, Long]) = {
    kafkaParam("group.id") = consumer
    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))
  }


  /**
    * =====发送消息=====
    */
  var producer: KafkaProducer[String, String] = null

  def getKafkaProducer: KafkaProducer[String, String] = {

    val properties = new Properties
    properties.put("bootstrap.servers", KAFKA_SERVER)
    properties.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idempotence", (true: java.lang.Boolean))

    var kafkaProducer: KafkaProducer[String, String] = null
    try {
      kafkaProducer = new KafkaProducer[String, String](properties)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    kafkaProducer

  }

  /**
    * @desc 发送消息
    * @param: topic
    * @param: msg
    */
  def send(topic: String, msg: String): Unit = {
    if (producer == null)
      producer = getKafkaProducer
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
    * @desc 发送消息
    * @param: topic
    * @param: msg
    * @param: key
    */
  def send(topic: String, msg: String, key: String): Unit = {
    if (producer == null)
      producer = getKafkaProducer
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }


  //写入kafka
  def main(args: Array[String]): Unit = {

    val topic = "rating_topic"

    val conf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

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

    val resDF01 = spark.sql(
      """
        |select a.title, b.*
        |from movies_info a
        |join ratings_info b on a.movieId = b.movieId
      """.stripMargin)
//    resDF01.show()

    resDF01.as[(String, String, String, String, String)].foreach { x =>
      send(topic, x.toString.replace(")","").replace("(",""))
      println(x)
      Thread.sleep(1000)
    }

  }

}
