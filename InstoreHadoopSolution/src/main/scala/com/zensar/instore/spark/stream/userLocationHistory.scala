package com.zensar.instore.spark.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import java.util.Formatter.DateTime
import java.util.Calendar
import org.apache.spark.SparkContext
import com.datastax.spark.connector.SomeColumns

import org.apache.spark.sql.cassandra
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnector._
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.rdd.RDD

object userLocationHistory {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true).setAppName("InStore")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
      .set("spark.driver.allowMultipleContexts", "true")
      
    val sc = new SparkContext(conf)
    
    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ec2-35-160-140-182.us-west-2.compute.amazonaws.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("mytopic")
    
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
      stream.foreachRDD { rdd =>
      val x = rdd.map(f => (f.key().toString(), f.value().toString().split(",")(1), f.value().toString().split(",")(2)))
      x.saveToCassandra("test", "customer", SomeColumns("loyalty", "category", "createddate"))
    }
    
    ssc.start();
    ssc.awaitTermination();
  }

  def convertToPut(key: String, value: String, sc: SparkContext): Unit = {
    // val collection = sc.parallelize(Seq(("key7", "73","asd"), ("key74", "74","asdasd")))
    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy hh:mm aa")
    var submittedDateConvert = new Date()
    val submittedAt = dateFormatter.format(submittedDateConvert).toString()
    val collection = sc.parallelize(Seq((key, value, submittedAt)))
    collection.saveToCassandra("test", "customer", SomeColumns("loyalty", "category", "createddate"))
  }

}

