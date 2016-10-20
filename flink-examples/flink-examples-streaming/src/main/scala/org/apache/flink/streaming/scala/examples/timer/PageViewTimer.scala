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
package org.apache.flink.streaming.scala.examples.timer

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{TimeCharacteristic, TimeDomain, TimerService}
import org.apache.flink.streaming.api.functions.TimelyFlatMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

import scala.collection.mutable

object PageViewTimer {

  var buffer = mutable.ArrayBuffer.empty[PageView]

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val pvs = List[PageView](
      PageView(0, "Jack", "http://foo", 1L),
      PageView(0, "Jack", "http://foo", 5L),
      PageView(0, "Jack", "http://foo/bar", 3L),
      PageView(0, "Jack", "http://foo/bar", 10L)
    )

    val dataStream: DataStream[PageView] = env.addSource(new SourceFunction[PageView]() {
      override def run(ctx: SourceContext[PageView]): Unit = {
        pvs.foreach(ctx.collect)
      }

      override def cancel(): Unit = {}
    })

    dataStream
      .keyBy("sessionId", "userId")
      .flatMap(new TimelyFlatMapFunction[PageView, UserTrajectory] {

        override def flatMap(pv: PageView, timerService: TimerService,
          out: Collector[UserTrajectory]): Unit = {
          buffer += pv
          timerService.registerEventTimeTimer(pv.timestamp - 1L)
        }

        override def onTimer(timestamp: Long, timeDomain: TimeDomain, timerService: TimerService,
          out: Collector[UserTrajectory]): Unit = {
          val trajectory = buffer.filter(_.timestamp <= timestamp).sortBy(_.timestamp).toList
          if (trajectory.nonEmpty) {
            val head = trajectory.head
            out.collect(UserTrajectory(head.sessionId, head.userId, trajectory, timestamp))
          }

        }
      }
      ).print()

    env.execute("PageView")
  }


  // simplified page view data model
  case class PageView(sessionId: Int, userId: String, url: String, timestamp: Long)

  case class UserTrajectory(sessionId: Int, userId: String,
                            trajectory: List[PageView], timestamp: Long)
}
