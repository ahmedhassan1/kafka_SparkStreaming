import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}








object kafkar {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("program started")

    val conf = new SparkConf().setMaster("local[4]").setAppName("kafkar")
    val ssc = new StreamingContext(conf, Seconds(2))

    // my kafka topic name is 'test'
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("twitter1" -> 5))

    kafkaStream.print()
    //kafkaStream.saveAsTextFiles("/home/cloudera/Desktop/tst/f.txt")
    //kafkaStream.format("parquet").option("path","your path")
    // kafkaStream.foreachRDD()


    ssc.start



    ssc.awaitTermination()

  }
}
