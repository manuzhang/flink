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

import scala.annotation.tailrec
import scala.util.Random


object PageViewGenerator {

  val GAP: Int = new Random().nextInt(20)
  val LATENESS = Distribution[Boolean](List(false -> 0.8, true -> 0.2))
  val PAGES = Distribution[String](
    List("foo.com" -> 0.35, "foo.news.com" -> 0.30, "foo.contact.com" -> 0.35))
  val USER_ID = Distribution.intEvenDistribution(0, 1)
  private var timestamp: Long = 1000L
  var lateRate = LateRate(0, 0)

  def genPageView: PageView = {
    val page: String = pickFromDistribution(PAGES)
    val userId: Int = pickFromDistribution(USER_ID)
    val late: Boolean = pickFromDistribution(LATENESS)
    if (late) {
      lateRate = LateRate(lateRate.count + 1, lateRate.total + 1)
      PageView(userId, page, timestamp - genInterval)
    } else {
      lateRate = LateRate(lateRate.count, lateRate.total + 1)
      timestamp += genInterval
      PageView(userId, page, timestamp)
    }
  }

  def genWatermark(timestamp: Long): Long = {
    timestamp - new Random().nextInt(GAP / 2)
  }

  private def genInterval: Long = {
    new Random().nextInt(2 * GAP)
  }

  private def pickFromDistribution[T](dist: Distribution[T]): T = {
    val rand = new Random().nextDouble()

    @tailrec
    def pick(data: List[(T, Double)], total: Double): T = {
      val (d, prop) = data.head
      val nextTotal = total + prop
      if (nextTotal >= rand) {
        d
      } else {
        pick(data.tail, nextTotal)
      }
    }

    pick(dist.data, 0.0)
  }

  // simplified page view data model
  case class PageView(userId: Int, url: String, timestamp: Long)
    extends Ordering[PageView] {
    override def compare(x: PageView, y: PageView): Int = {
      x.timestamp.compareTo(y.timestamp)
    }
  }

  case class Distribution[T](data: List[(T, Double)])

  object Distribution {

    def intEvenDistribution(start: Int, end: Int): Distribution[Int] = {
      if (start >= end) {
        throw new IllegalArgumentException(
          s"invalid arguments [$start, $end) to generate even distribution")
      }

      Distribution[Int]((start until end).map(_ -> 1.0 / (end - start)).toList)
    }
  }

  case class LateRate(count: Int, total: Int)
}
