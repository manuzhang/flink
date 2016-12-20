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
package org.apache.flink.streaming.scala.examples.session

import com.fasterxml.jackson.databind.ObjectMapper
import com.vip.data.cleaning.logic.mars.MobilePageProto.{MobilePage, SourceFrom}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SourceFromSessionWindowing {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)

    val inputFormat = new MobilePageCsvInputFormat(new Path("page.csv"))

    val dataStream = env.readFile(inputFormat, "page.csv")

/*    val dataStream: DataStream[MobilePage] = env.addSource(new ParallelSourceFunction[MobilePage]() {

      override def run(ctx: SourceContext[MobilePage]): Unit = {
        var watermark = 0L
        var count = 0
        while (count < 100) {
          val page = MobilePageGenerator.genMobilePage
          // TODO: page_start_time, page_end_time, page_loaded_time or triggertime?
          val eventTime = getEventTime(page)
          val wm = MobilePageGenerator.genWatermark(eventTime)
          ctx.collectWithTimestamp(page, eventTime)
          if (wm > watermark) {
            watermark = wm
            ctx.emitWatermark(new Watermark(watermark))
          }
          count += 1
        }
      }

      override def cancel(): Unit = {
      }
    })*/

    dataStream.filter(!_.getSourceFromOrigin.equals("-99")).writeAsText("page_views", FileSystem.WriteMode.OVERWRITE)


/*    dataStream
      .keyBy(_.getUserid)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(MobilePageGenerator.GAP)))
      .trigger(PerElementEventTimeTrigger.create())
      .apply(new SortAndEmitFn).writeAsText("user_trajectory", FileSystem.WriteMode.OVERWRITE)*/

    env.execute("PageView")
  }

  private def getEventTime(mobilePage: MobilePage): Long = {
    mobilePage.getPageStartTime
  }

  class SortAndEmitFn extends ProcessWindowFunction[MobilePage, UserTrajectory, String, TimeWindow] {

    override def process(userId: String, input: Iterable[MobilePage],
      context: Context,
      out: Collector[UserTrajectory]): Unit = {
      val trajectory = input.filter(getEventTime(_) <= context.watermark).toList.sortBy(getEventTime)
      out.collect(UserTrajectory(userId, trajectory, context.window, context.watermark))
    }
  }



  case class UserTrajectory(userId: String, trajectory: List[MobilePage], window: TimeWindow,
                            watermark: Long)
}
