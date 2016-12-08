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

import java.util.UUID

import com.vip.data.cleaning.logic.mars.mobile.page.MobilePageProtos.{MobilePage, SourceFrom}

import scala.util.Random

object MobilePageGenerator extends DistributionGenerator {

  private val random = new Random(System.currentTimeMillis())
  val GAP: Int = random.nextInt(20)
  private val lateDist = Distribution[Boolean](List(false -> 0.8, true -> 0.2))
  private var timestamp: Long = 1000L
  private var lateRate = LateRate(0, 0)

  def genMobilePage: MobilePage = {
    val late: Boolean = pickFromDistribution(lateDist)
    if (late) {
      lateRate = LateRate(lateRate.count + 1, lateRate.total + 1)
    } else {
      lateRate = LateRate(lateRate.count, lateRate.total + 1)
      timestamp += genInterval
    }

    val sourceFrom: SourceFrom = SourceFromGenerator.genSourceFrom
    MobilePage.newBuilder()
      .setPageId(random.nextLong())
      .setPageLoadedTime(timestamp)
      .setUserid(UUID.randomUUID().toString)
      .setSourceFrom(sourceFrom)
      .build()
  }

  def genWatermark(timestamp: Long): Long = {
    timestamp - new Random().nextInt(GAP / 2)
  }

  private def genInterval: Long = {
    new Random().nextInt(2 * GAP)
  }

  case class LateRate(count: Int, total: Int)
}

object SourceFromGenerator extends DistributionGenerator {

  def genStart: Int = {
    pickFromDistribution(
      intEvenDistribution((1 to 10) :+ (-99) :_*)
    )
  }

  def genStartFrom(start: Int): Int = {
    start match {
      case 1 =>
        pickFromDistribution(intEvenDistribution(1, 2, 9, 13))
      case 4 =>
        pickFromDistribution(intEvenDistribution(3, 4, 5, 6, 13))
      case 5 => 6
      case 6 => 7
      case 9 =>
        pickFromDistribution(intEvenDistribution(10, 11, 12))
      case _ => -99
    }
  }


  def genSourceFrom: SourceFrom = {
    val start = genStart
    SourceFrom.newBuilder()
      .setStart(start.toString)
      .setStartFrom(genStartFrom(start).toString)
      .build()
  }

}
