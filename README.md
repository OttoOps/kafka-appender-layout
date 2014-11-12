kafka-appender-layout
=====================

A subclass of the Kafka log4j appender with RegEx hard-coded for Druid

  Example event (input):
   "Event [{"feed":"metrics","timestamp":"2014-07-29T16:23:26.240Z","service":"historical","host":"needle26.lab.mystrotv.com:8081","metric":"sys/swap/pageIn","value":0}]";
  Example output:
   {"source":"druid","sourceVersion":"0.6.129","sourceCluster":"Escher","feed":"metrics","timestamp":"2014-06-10T19:58:04.384Z","service":"middlemanager-rt1","host":"needle13.lab.mystrotv.com:8088","metricName":"jvm/bufferpool/count","value":244,"user2":"mapped"}
  

To use this appender, you'll need to:

1.  Build the project
2.  Make the generated jar file available on the classpath of the application which needs to log to Kafka
3.  Ensure your log4j properties file specifies "com.twc.needle.LayoutKafkaLog4jAppender" in place of "kafka.producer.KafkaLog4jAppender"
4.  Properties file example:

```
log4j.logger.com.metamx.emitter.core.LoggingEmitter=INFO, KAFKA
log4j.appender.KAFKA=com.twc.needle.LayoutKafkaLog4jAppender
log4j.appender.KAFKA.ClientSource=druid
log4j.appender.KAFKA.ClientSourceVersion=0.6.129
log4j.appender.KAFKA.ClientSourceCluster=Escher
log4j.appender.KAFKA.BrokerList=needle26:9092
log4j.appender.KAFKA.Topic=operational_stats
```
