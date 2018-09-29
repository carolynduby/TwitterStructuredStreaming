package example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.streaming.Trigger
 
object TwitterStreamHive extends TwitterStreamingHiveSparkApp with App {

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
               .option("kafka.bootstrap.servers", "ip-172-31-8-21.eu-west-2.compute.internal:6667")
               .option("subscribe", "simple_tweets")
               .load()
   
   kafka.printSchema()

val simplified_tweet_schema = new StructType()
                   .add("twitter_created_at", StringType)
                   .add("twitter_text", StringType)
                   .add("twitter_lang", StringType)
                   .add("twitter_full_text", StringType)
                   .add("twitter_longitude", StringType)
                   .add("twitter_latitude", StringType)
                   .add("twitter_screen_name", StringType)
                   .add("twitter_hashtags", StringType)
                   .add("twitter_retweet_count", StringType)

   val rawTweets =  kafka.select(from_json(col("value").cast("string"), simplified_tweet_schema).alias("parsed_value"))
   

   val query = rawTweets.select(col("parsed_value.twitter_retweet_count").cast("int").alias("retweet_count"), 
                                col("parsed_value.twitter_screen_name").alias("screen_name"), 
                                col("parsed_value.twitter_text").alias("text"), 
                                to_timestamp(col("parsed_value.twitter_created_at"), "EEE MMM dd kk:mm:ss +SSSS yyyy").alias("created_at"), 
                                col("parsed_value.twitter_full_text").alias("full_text"), 
                                col("parsed_value.twitter_latitude").cast("int").alias("latitude"), 
                                col("parsed_value.twitter_longitude").cast("int").alias("longitude"), 
                                col("parsed_value.twitter_hashtags").alias("hashtags"))
                   .writeStream
                   .trigger(Trigger.ProcessingTime("2 seconds"))
//                   .format("console")
//                   .start 

                   .format("orc")
                   .option("checkpointLocation", "hdfs://ip-172-31-8-21:8020/user/spark/twitterStreamHive/checkpoint")
                   .option("path", "s3a://cdubytwitterlondon/orc")
                   .start()

     val split_json_array = udf((json_array: String) => {
                      json_array.substring(1, json_array.length - 1).split(",").map(_.trim)
                      })

   /*  val trim_string = udf((raw_string: String) => {
                          if (raw_string.trim.length() == 0) {
                              lit(null:String)
                          } else {
                              raw_string
                          }
                      })*/

   val druid_query = rawTweets.select(col("parsed_value.twitter_retweet_count").cast("int").alias("retweet_count"),
                                col("parsed_value.twitter_screen_name").alias("screen_name"),
                                col("parsed_value.twitter_text").alias("text"),
                                to_timestamp(col("parsed_value.twitter_created_at"), "EEE MMM dd kk:mm:ss +SSSS yyyy").alias("created_at"),
                                col("parsed_value.twitter_full_text").alias("full_text"),
                                col("parsed_value.twitter_latitude").cast("int").alias("latitude"),
                                col("parsed_value.twitter_longitude").cast("int").alias("longitude"),
                                col("parsed_value.twitter_hashtags").alias("hashtags"))
                   .withColumn("hashtags", split_json_array($"hashtags"))
                   .withColumn("hashtags", explode($"hashtags"))
                   .filter(length($"hashtags") > 0 )
                   .writeStream
                   .trigger(Trigger.ProcessingTime("2 seconds"))
                   .format("orc")
                   .option("checkpointLocation", "hdfs://ip-172-31-8-21:8020/user/spark/twitterStreamHive/checkpoint/hashtags")
                   .option("path", "s3a://cdubytwitterlondon/orc/hashtags")
                   .start

    spark.streams.awaitAnyTermination() 

}

trait TwitterStreamingHiveSparkApp {
  lazy val appName = "Twitter Streaming Spark App"
}
