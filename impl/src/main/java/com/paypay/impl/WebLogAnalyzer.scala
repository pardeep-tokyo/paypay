package com.paypay.impl

import java.text.SimpleDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window

object WebLogAnalyzer {
  
     def main(args:Array[String]):Unit={
       
       if(args.length <2){
         println("Please provide the web log file <input path> <output path> ")
        System.exit(1);
       }
        val filePath = args(0);
        val outputPath = args(1);
       
       Logger.getRootLogger.setLevel(Level.ERROR)
       Logger.getLogger("org").setLevel(Level.OFF)
       Logger.getLogger("akka").setLevel(Level.OFF)
       
    val sc = new SparkContext("local[*]", "WebLogAnalyzer")

    //converting the text file into RDD
    val logRecords = sc.textFile(filePath);
    println("Sample records from the web log file")
    logRecords.take(10).foreach(println)
    println("******************************")
    //parsing each line of file and transforming it
    val parsedLogLines= logRecords.map(parseLogLine)

    
    //filter any data that is not considered as session and only take timestamp, userIp, url,user_agent, epoch
    var filterLogLines=parsedLogLines.filter(logRecord=>logRecord.backendIp!="-" && logRecord.request_processing_time!="-1" && logRecord.backend_processing_time!="-1" &&
      logRecord.response_processing_time!="-1").map(logRecord=>(logRecord.timestamp,logRecord.clientIp,logRecord.request,logRecord.user_agent,addEpochDate(logRecord.timestamp)))
    //adding schema to log Rdd using Case Class WebLogDF
    val webLogWithSchema= filterLogLines.map(webLog=>WebLogDF(webLog._1,webLog._2,webLog._3,webLog._4,webLog._5))
    
    // transforming WebLogDF RDD  to Row RDD  
    val rowRDD = webLogWithSchema.map(x => Row(x.timestamp, x.userIp, x.url, x.userAgent, x.epoch))
    
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    // schema to covert rowRDD into DataFrame  
    val schema = new StructType()
      .add(StructField("timestamp", StringType, true))
      .add(StructField("userIp", StringType, true))
      .add(StructField("url", StringType, true))
      .add(StructField("userAgent",StringType,true))
      .add(StructField("epoch", LongType, true))
    
    //coverting rowRDD to DataFrame 
    val webLogDf= spark.sqlContext.createDataFrame(rowRDD,schema)

    println("\nSample records after the web logs are loaded into dataframes using the scheme ")
    println(" schema -> timestamp, userIp,url,userAgent,epoch \n")
    webLogDf.show(10)
    println("******************************\n")

    //define windowing scheme
    val windowSpec = Window.partitionBy("userIp").orderBy("epoch")
    //udf for deciding if new session began(15 mins)
    val isNewSession = udf((duration: Long) => {
      if (duration > 900000) 1
      else 0
    })

    //Define a udf to concatenate two passed in string values
    val getConcatenated = udf( (first: String, second: String) => { first + "_" + second } )
    //using window function getting the previous epoch using lag
    val webLogDfWithEpoch= webLogDf.withColumn("prevEpoch",lag(webLogDf("epoch"), 1).over(windowSpec))
    //cleaning epoch column by removing nulls
    val webLogDfWithEpochCleaned= webLogDfWithEpoch.withColumn("prevEpoch_cleaned", coalesce(col("prevEpoch"), col("epoch")))

    //calculating duration
    val webLogDfWithDuration= webLogDfWithEpochCleaned.withColumn("duration_miliseconds",webLogDfWithEpochCleaned("epoch")-webLogDfWithEpochCleaned("prevEpoch_cleaned"))
    //adding isNewSession column using a helper function isNewSession
    val webLogDfWithNewSessionFlag= webLogDfWithDuration.withColumn("isNewSession",isNewSession(col("duration_miliseconds")))
    //adding window index column
    val webLogDfWithWindowIdx=webLogDfWithNewSessionFlag.withColumn("windowIdx",sum("isNewSession").over(windowSpec).cast("string"))
    //adding a new column SessionId by concatinating  index from Window function+ userIp
    //val webLogDfWithSessionId=webLogDfWithWindowIdx.withColumn("sessionId",getConcatenated($"userIp",$"windowIdx")).cache()
    val webLogDfWithSessionId=webLogDfWithWindowIdx.withColumn("sessionId",getConcatenated(col("userIp"),col("windowIdx"))).select("userIp","sessionId","duration_miliseconds","url","userAgent").cache()

    //Part 1 Sessionize the web log by IP and write to hdfs
    println("\nSessionize the web log by IP and printing 10 sample values on console")
    webLogDfWithSessionId.show(10)
    println("******************************\n")
    
    println("\nSessionize the web log by IP and writing to hdfs path ="+outputPath+"/SessionizeByIP/")
    webLogDfWithSessionId.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save(outputPath+"/SessionizeByIP/")
    println("******************************\n")
    
    //Part 2 Determine the average session time and write to hdfs
    println("\nDetermine the average session time and printing average session time on console")
    webLogDfWithSessionId.select(mean("duration_miliseconds")).show(1)
    println("******************************\n")
    println("\nDetermine the average session time and writing to hdfs path ="+outputPath+"/AverageSessionTime/")
    webLogDfWithSessionId.select(mean("duration_miliseconds")).write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save(outputPath+"/AverageSessionTime/")
    println("******************************\n")
    
    //Part 3 Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session and write to hdfs
    val urlVisitsPerSessionBase=webLogDfWithSessionId.select("sessionId","url").groupBy(col("sessionId")).agg((collect_set("url")))
    val urlVisitsPerSession= urlVisitsPerSessionBase.select(col("sessionId"),size(col("collect_set(url)")))
    println("\n Determine unique URL visits per session and printing 10 sample values on console ")
    urlVisitsPerSession.show(10)
    println("******************************\n")
    println("\n Determine unique URL visits per session and writing to hdfs path ="+outputPath+"/UniqueURLsPerSession/")
    urlVisitsPerSession.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save(outputPath+"/UniqueURLsPerSession/")
     println("******************************\n")
    
    //Part 4: Find the most engaged users, ie the IPs with the longest session times and write to hdfs
     println("\n Find the most engaged users, ie the IPs with the longest session times and printing 10 sample values on console")
    webLogDfWithSessionId.groupBy("userIp").sum("duration_miliseconds").sort(col("sum(duration_miliseconds)").desc).show(10)
     println("******************************\n")
     println("\n Find the most engaged users, ie the IPs with the longest session times and writing to hdfs path ="+outputPath+"/UsersWithLongestSessionTime/")
    webLogDfWithSessionId.groupBy("userIp").sum("duration_miliseconds").sort(col("sum(duration_miliseconds)").desc).repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save(outputPath+"/UsersWithLongestSessionTime/")
     println("******************************\n")
     }
     
     
         //case class for weblogDF schema
  case class WebLogDF(timestamp: String, userIp: String, url: String, userAgent:String, epoch:Long)
  //case class for the log file
  case class WebRecord(timestamp: String, elb: String, clientIp: String, backendIp:String ,request_processing_time: String, backend_processing_time: String
                       , response_processing_time:String, elb_status_code:String, backend_status_code:String, received_bytes:String
                       , sent_bytes:String, request:String, user_agent:String, ssl_cipher:String
                       , ssl_protocol:String)
  //2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
  //2015-07-22T09:00:27.894580Z marketpalce-shop 203.91.211.44:51402 10.0.4.150:80 0.000024 0.15334 0.000026 200 200 0 1497 "GET https://paytm.com:443/shop/wallet/txnhistory?page_size=10&page_number=0&channel=web&version=2 HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; rv:39.0) Gecko/20100101 Firefox/39.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2

  //defining regex pattern for each log file line for parsing
  val PATTERN = """^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r

  //converting the datetime to epoch date for calculating session durations
  def addEpochDate(date:String)= {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    val dt = df.parse(date);
    val epoch = dt.getTime();
    (epoch)

  }
  
  //parsing each record in log file using the function
  def parseLogLine(log: String): WebRecord = {
    try {
      val res = PATTERN.findFirstMatchIn(log)

      if (res.isEmpty) {
        println("Rejected Log Line: " + log)
        WebRecord("Empty", "-", "-", "", "",  "", "", "-", "-","","","","","","" )
      }
      else {
        val m = res.get
        if (m.group(4).equals("-")) {
          WebRecord(m.group(1), m.group(2), m.group(3),"",
            m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13)
            , m.group(14), m.group(15))
        }
        else {
          WebRecord(m.group(1), m.group(2), m.group(3),m.group(4),
            m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13)
            , m.group(14), m.group(15))
        }
      }
    } catch
      {
        case e: Exception =>
          println("Exception on line:" + log + ":" + e.getMessage);
          WebRecord("Empty", "-", "-", "", "",  "", "", "-", "-","","","","","","" )
      }
  }
}