/*
sandesh
 */
package myflink

import datatypes.{GeoPoint, TaxiRide}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import sources.TaxiRideSource
import org.apache.flink.api.scala._
import _root_.utils.NycGeoUtils
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SlidingArrivalCount {
  def main(args: Array[String]){
    val data = "./data/nycTaxiData.gz"
    val maxServingDelay = 60
    val servingSpeedFactor = 600f

    // window parameters
    val countWindowLength = 15 // window size in min
    val countWindowFrequency =  5 // window trigger interval in min

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rides: DataStream[TaxiRide] = env.addSource(new TaxiRideSource(
      data, maxServingDelay, servingSpeedFactor))

    val cleansedRides = rides.filter(!_.isStart)
      .filter(r => NycGeoUtils.isInNYC(r.location))

    val cellIds: DataStream[(Int, Short)] = cleansedRides
      .map(r => (NycGeoUtils.mapToGridCell(r.location), r.passengerCnt))

    val passengerCnts: DataStream[(Int, Long, Int)] = cellIds
      .keyBy(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(countWindowLength),
        Time.seconds(countWindowFrequency)))
      .apply { (
                 cell: Int,
                 window: TimeWindow,
                 events: Iterable[(Int, Short)],
                 out: Collector[(Int, Long, Int)]) =>
        out.collect( ( cell, window.getEnd, events.map( _._2 ).sum ) )
      }

    val cntByLocation: DataStream[(Int, Long, GeoPoint, Int)] = passengerCnts
      .map( r => ( r._1, r._2, NycGeoUtils.getGridCellCenter(r._1), r._3 ) )

    cntByLocation.print()

    env.execute("SlidingArrivalCount of passenger")
  }

}
