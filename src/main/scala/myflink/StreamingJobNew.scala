/*
 * Sandesh
 */

package myflink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingJobNew {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rawText = env.socketTextStream("localhost", 9000, '\n')
      .assignTimestampsAndWatermarks(new TimestampExtractor)

    val counts = rawText.map { (text: String) => (text.split(",")(0), text.split(",")(1), 1) }
      .keyBy(1)
      .timeWindow(Time.seconds(20), Time.seconds(10))
      .sum(2)

    counts.print()

    env.execute("Flink Streaming Scala API Skeleton")

  }
//timeStamp, word
  class TimestampExtractor extends AssignerWithPeriodicWatermarks[String] with Serializable {

    override def extractTimestamp(e: String, prevElementTimestamp: Long) = {
      e.split(",")(0).toLong
    }

    override def getCurrentWatermark(): Watermark = {
      new Watermark(System.currentTimeMillis - 10000)
    }

  }

}


//counts.map(x => { println(System.currentTimeMillis()) }).print()