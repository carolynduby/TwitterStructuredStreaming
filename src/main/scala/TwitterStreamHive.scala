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
               .option("kafka.bootstrap.servers", "localhost:6667")
               .option("subscribe", "simple_tweets")
               .load()
   
   kafka.printSchema()

   /*val simplified_tweet_schema = new StructType()
                   .add("twitter_created_at", StringType)
                   .add("twitter_text", StringType)
                   .add("twitter_lang", StringType)
                   .add("twitter_full_text", StringType)
                   .add("twitter_longitude", IntegerType) 
                   .add("twitter_latitude", IntegerType)
                   .add("twitter_screen_name", StringType)
                   .add("twitter_hashtags", ArrayType(StringType))
                   .add("twitter_retweet_count", IntegerType)  
*/

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

   //val rawTweets =  kafka.select(from_json(col("value").cast("string"), simplified_tweet_schema))
   val rawTweets =  kafka.select(from_json(col("value").cast("string"), simplified_tweet_schema).alias("parsed_value"))
   

   val query = rawTweets.select(col("parsed_value.twitter_retweet_count").alias("retweet_count"), 
                                col("parsed_value.twitter_screen_name").alias("screen_name"), 
                                col("parsed_value.twitter_text").alias("text"), 
                                col("parsed_value.twitter_created_at").alias("created_at"), 
                                col("parsed_value.twitter_full_text").alias("full_text"), 
                                col("parsed_value.twitter_latitude").alias("latitude"), 
                                col("parsed_value.twitter_longitude").alias("longitude"), 
                                col("parsed_value.twitter_hashtags").alias("hashtags"))
                   .writeStream
                   .trigger(Trigger.ProcessingTime("2 seconds"))
                   .format("orc")
                   .option("checkpointLocation", "hdfs://localhost:8020/user/spark/twitterStreamHive/checkpoint")
                   .option("path", "s3a://cdubytwitter/orc")
                   .start()

    query.awaitTermination() 

}

trait TwitterStreamingHiveSparkApp {
  lazy val appName = "Twitter Streaming Spark App"
}
