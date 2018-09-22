package example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.streaming.Trigger
 
object TwitterStream extends TwitterStreamingSparkApp with App {

   val spark = SparkSession
                   .builder
                   .appName(appName)
                   .getOrCreate()

   // implicit conversions for SparkSession above - RDDs to DataFrames
   import spark.implicits._

   println(spark.version)
  
   val kafka = spark
               .readStream
               .format("kafka")
               .option("kafka.bootstrap.servers", "localhost:6667")
               .option("subscribe", "tweets")
               .load()

   val rawTweets =  kafka.selectExpr("CAST(value AS STRING)").as[(String)]


    // Generate running word count

    val query = rawTweets.writeStream
                   .format("text")
                   .trigger(Trigger.ProcessingTime("2 seconds"))
                   .option("checkpointLocation", "hdfs://localhost:8020/user/spark/twitterStream/checkpoint")
                   .option("path", "s3a://cdubytwitter/raw")
                   .start() 

    query.awaitTermination() 

}

trait TwitterStreamingSparkApp {
  lazy val appName = "Twitter Streaming Spark App"
}
