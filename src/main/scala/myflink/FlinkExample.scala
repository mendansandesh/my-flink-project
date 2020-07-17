/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myflink

import java.util.Properties
import java.util.concurrent.TimeUnit


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08, FlinkKafkaProducer08}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

object FlinkExample {
  val stopWords = Set("a", "an", "the")
  val window = Time.of(10, TimeUnit.SECONDS)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    val kafkaConsumer =
      new FlinkKafkaConsumer08[String]("tweets", new SimpleStringSchema(), properties)

    /*val kafkaProducer = new FlinkKafkaProducer08[String](
      "localhost:9092",
      "output",
      KafkaStringSchema
    )*/

    val lines = env.addSource(kafkaConsumer)

    val wordCounts = lines
      .flatMap(line => line.split(" "))
      .filter(word => !word.isEmpty)
      .map(word => word.toLowerCase)
      .filter(word => !stopWords.contains(word))
      .map(word => (word, 1))
      .keyBy(0)
      .timeWindow(window)
      .sum(1)

    wordCounts.print()

    env.execute("Test flink")
  }

  /*object KafkaStringSchema extends SerializationSchema[String, Array[Byte]] with
    DeserializationSchema[String] {

    import org.apache.flink.api.common.typeinfo.TypeInformation
    import org.apache.flink.api.java.typeutils.TypeExtractor

    override def serialize(t: String): Array[Byte] = t.getBytes("UTF-8")

    override def isEndOfStream(t: String): Boolean = false

    override def deserialize(bytes: Array[Byte]): String = new String(bytes, "UTF-8")

    override def getProducedType: TypeInformation[String] =
      TypeExtractor.getForClass(classOf[String])
  }*/
}
