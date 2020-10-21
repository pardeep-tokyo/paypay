# PayPay Coding Challenge

Web Log data analysis is done using Spark Scala. 
This is a data pipe line implementated for web log data analysis in Cloudera Hadoop using Spark,Scala, Spark-SQL, DataFrame, and HDFS.

1. WebLogAnalyzer.scala : Spark implementation for parsing weblogs given in log file. These are webserver logs. There are 4 tasks that are to be done by this spark scala script.       
    Part 1 : Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window. 
    Part 2: Determine the average session time 
    Part 3: Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session. 
    Part 4: Find the most engaged users, ie the IPs with the longest session times Session window time is chosen to be 15 minutes. Please update the values of the       variables as per your implementation

2. The log file was taken from https://github.com/Pay-Baymax/DataEngineerChallenge/tree/master/data ,
    an AWS Elastic Load Balancer format: http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format 
3. pom.xml: Maven dependency file
4. download the above code and copy the absolute path to the target folder
5. spark-submit  --class "com.paypay.impl.WebLogAnalyzer" --master local[*]   /absolutepath/impl-0.0.1-SNAPSHOT.jar  <hdfs input folder path with log file> <hdfs output folder path>
   e.g.        spark-submit  --class "com.paypay.impl.WebLogAnalyzer" --master local[*]   /home/cloudera/Downloads/scalaworkspace/impl/target/impl-0.0.1-SNAPSHOT.jar  hdfs://localhost:8020/user/cloudera/input hdfs://localhost:8020/user/cloudera/output
  
6. once the spark-submit job is completed four output directories will be created for  each part
    Part 1 : Sessionize the web log by IP --> <hdfs output folder path>/SessionizeByIP/
            hdfspath =hdfs://localhost:8020/user/cloudera/output/SessionizeByIP/
  
    Part 2: Determine the average session time --> <hdfs output folder path>/AverageSessionTime/
            hdfspath =hdfs://localhost:8020/user/cloudera/output/AverageSessionTime/
  
    Part 3: Determine unique URL visits per session --> <hdfs output folder path>/UniqueURLsPerSession/ 
            hdfspath =hdfs://localhost:8020/user/cloudera/output/UniqueURLsPerSession/
  
    Part 4: Find the most engaged users, ie the IPs with the longest session times Session window time is chosen to be 15 minutes 
               --> <hdfs output folder path>/UsersWithLongestSessionTime/
            hdfspath =hdfs://localhost:8020/user/cloudera/output/UsersWithLongestSessionTime/
