/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myflink

import datatypes.{GeoPoint, TaxiRide}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import sources.TaxiRideSource
import org.apache.flink.api.scala._
import _root_.utils.NycGeoUtils
import org.apache.flink.api.common.functions.ReduceFunction
/**
 * Apache Flink DataStream API demo application.
 *
 * The program processes a stream of taxi ride events from the New York City Taxi and Limousine
 * Commission (TLC).
 * It computes for each location the total number of persons that arrived by taxi.
 *
 * See
 *   http://github.com/dataartisans/flink-streaming-demo
 * for more detail.
 *
 */
object TotalArrivalCount {

  def main(args: Array[String]) {

    // input parameters
    val data = "./data/nycTaxiData.gz"
    val maxServingDelay = 60
    val servingSpeedFactor = 600f


    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.enableCheckpointing(5000)

    // Define the data source
    val rides: DataStream[TaxiRide] = env.addSource(new TaxiRideSource(
      data, maxServingDelay, servingSpeedFactor))

    val cleansedRides = rides
      // filter for trip end events
      .filter( !_.isStart )
      // filter for events in NYC
      .filter( r => NycGeoUtils.isInNYC(r.location) )

    // map location coordinates to cell Id, timestamp, and passenger count
    val cellIds: DataStream[(Int, Long, Short)] = cleansedRides
      .map { r =>
        ( NycGeoUtils.mapToGridCell(r.location), r.time, r.passengerCnt )
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
      .map( r => (r._1, r._2, NycGeoUtils.getGridCellCenter(r._1), r._3 ) )

    // print to console
    cntByLocation
      .print()


    env.execute("Total passenger count per location")

  }

}
