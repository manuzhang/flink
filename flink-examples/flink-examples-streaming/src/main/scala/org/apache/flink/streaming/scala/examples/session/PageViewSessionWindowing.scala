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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.scala.examples.session.PageViewGenerator.PageView
import org.apache.flink.util.Collector



object PageViewSessionWindowing {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val gap = Time.milliseconds(params.getLong("gap", 10))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)

    val dataStream: DataStream[PageView] = env.addSource(new SourceFunction[PageView]() {

      override def run(ctx: SourceContext[PageView]): Unit = {
        var watermark = 0L
        var count = 0
        while (count < 100) {
          val pv = PageViewGenerator.genPageView
          val wm = PageViewGenerator.genWatermark(pv.timestamp)
          ctx.collectWithTimestamp(pv, pv.timestamp)
          if (wm > watermark) {
            watermark = wm
            ctx.emitWatermark(new Watermark(watermark))
          }
          count += 1
        }
      }

      override def cancel(): Unit = {
      }
    })

    dataStream.writeAsText("page_views.txt", FileSystem.WriteMode.OVERWRITE)

    dataStream
      .keyBy(_.userId)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(PageViewGenerator.GAP)))
      .trigger(PerElementEventTimeTrigger.create())
      .apply(new SortAndEmitFn).writeAsText("user_trajectory.txt", FileSystem.WriteMode.OVERWRITE)

    env.execute("PageView")
    println(s"GAP: ${PageViewGenerator.GAP}")
    println(PageViewGenerator.lateRate)
  }

  class SortAndEmitFn extends ProcessWindowFunction[PageView, UserTrajectory, Int, TimeWindow] {

    override def process(userId: Int, input: Iterable[PageView],
      context: Context,
      out: Collector[UserTrajectory]): Unit = {
      val trajectory = input.filter(_.timestamp <= context.watermark).toList.sortBy(_.timestamp)
      out.collect(UserTrajectory(userId, trajectory, context.window, context.watermark))
    }
  }



  case class UserTrajectory(userId: Int, trajectory: List[PageView], window: TimeWindow,
                            watermark: Long)
}
