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

import datatypes.{GeoPoint, TaxiRide}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import sources.TaxiRideSource
import org.apache.flink.api.scala._
import _root_.utils.NycGeoUtils
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.function.util.ScalaFoldFunction


object TotalArrivalCount_old {
  def main(args: Array[String]) {
    /*implicit val typeInfo1 = TypeInformation.of(classOf[(TaxiRide)])
    implicit val typeInfo2 = TypeInformation.of(classOf[(Int, Long, Short)])
    implicit val typeInfo3 = TypeInformation.of(classOf[(Int)])
    implicit val typeInfo4 = TypeInformation.of(classOf[(Int, Long, GeoPoint, Int)])*/

    val data = "./data/nycTaxiData.gz"
    val maxServingDelay = 60
    val servingSpeedFactor = 600f

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rides: DataStream[TaxiRide] = env.addSource(new TaxiRideSource(
      data, maxServingDelay, servingSpeedFactor))

    val cleansedRides = rides
      // filter for trip end events
      .filter(!_.isStart)
      // filter for events in NYC
      .filter(r => NycGeoUtils.isInNYC(r.location))

    // map location coordinates to cell Id, timestamp, and passenger count
    val cellIds: DataStream[(Int, Long, Short)] = cleansedRides
      .map { r =>
        (NycGeoUtils.mapToGridCell(r.location), r.time, r.passengerCnt)
      }

    val passengerCnts: DataStream[(Int, Long, Short)] = cellIds
      // key stream by cell Id
      .keyBy(_._1)
      // sum passengers per cell Id and update time
      /*.fold((0, 0L, 0), (s: (Int, Long, Int), r: (Int, Long, Short)) =>
    { (r._1, s._2.max(r._2), s._3 + r._3) } )*/
      .reduce(new ReduceFunction[(Int, Long, Short)] {
        override def reduce(t: (Int, Long, Short), t1: (Int, Long, Short)): (Int, Long, Short) =
          (t1._1, t._2.max(t1._2), (t._3 + t1._3).toShort)
      })
    // map cell Id back to GeoPoint
    val cntByLocation: DataStream[(Int, Long, GeoPoint, Int)] = passengerCnts
      .map(r => (r._1, r._2, NycGeoUtils.getGridCellCenter(r._1), r._3.toInt))

    // print to console
    cntByLocation
      .print()
  }

}
