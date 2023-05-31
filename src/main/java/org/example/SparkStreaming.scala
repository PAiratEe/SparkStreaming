package org.example

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import java.io.IOException
import java.sql.DriverManager
import java.util
import java.util.Properties

object SparkStreaming extends scala.Serializable {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.exit(1)
    }
    val Array(brokers, groupId, topics) = args

    val sparkConf = new SparkConf().setAppName("Application").setMaster("local[2]").set("spark.executor.memory", "1g")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("hdfs://localhost:9000/kafkaTemplate/")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1000"
    )

    val bootstrapServers: String = "127.0.0.1:9092"

    val properties: Properties = new Properties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "1")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "200000")

    val producerConfig: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    producerConfig.put("bootstrap.servers", "localhost:9092")
    producerConfig.put("key.serializer", classOf[StringSerializer])
    producerConfig.put("value.serializer", classOf[StringSerializer])
    producerConfig.put("ack", "1")
    producerConfig.put("batch-size", "200000")
    producerConfig.put("guarantee", "at_least_once")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val lines = messages.map(_.value)
    val necessaryMessages = lines.filter(line =>
      line.contains("\"bot\":true")
    )
    val time = System.nanoTime
    var count = 0

    necessaryMessages.foreachRDD { rdd =>
      if (!rdd.isEmpty) {
        val connect = DriverManager.getConnection("jdbc:postgresql://localhost:5432/spark-streaming", properties)
        val statement = connect.createStatement()
        try {
          rdd.collect().foreach(unit => {
            count += 1
            val sql = s"INSERT INTO data (column2, column3) VALUES('${unit.substring(0, unit.indexOf("\n"))}', '${(System.nanoTime() - time) / 1e9d}')"
            statement.executeUpdate(sql)
          })
        }
        catch {
          case c: IOException =>
            println("Данная операция была прервана " + c.printStackTrace())
        }
        finally {
          statement.close()
          connect.close()
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
    println("///////" + count / ((System.nanoTime() - time) / 1e9d) + " tps")
  }

}