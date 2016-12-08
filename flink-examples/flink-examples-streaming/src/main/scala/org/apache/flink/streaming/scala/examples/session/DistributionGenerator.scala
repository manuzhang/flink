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

trait DistributionGenerator {

  def intEvenDistribution(numbers: Int*): Distribution[Int] = {
    Distribution(numbers.map(_ -> 1.0 / numbers.length).toList)
  }

  def pickFromDistribution[T](dist: Distribution[T]): T = {
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

  case class Distribution[T](data: List[(T, Double)])
}
