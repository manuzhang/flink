/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.scala.examples.session

import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.vip.data.cleaning.logic.mars.MobilePageProto.{MobilePage, SourceFrom}
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.scala.examples.session.MobilePageCsvInputFormat._

import scala.collection.JavaConversions._

object MobilePageCsvInputFormat {
  private val fields = MobilePage.getDescriptor.getFields

  private val types = fields.map(_.getJavaType match {
    case BOOLEAN => classOf[java.lang.Boolean]
    case DOUBLE => classOf[java.lang.Double]
    case FLOAT => classOf[java.lang.Float]
    case LONG => classOf[java.lang.Long]
    case INT => classOf[java.lang.Integer]
    case STRING => classOf[java.lang.String]
  })
}

class MobilePageCsvInputFormat(file: Path) extends CsvInputFormat[MobilePage](file) {
  setFieldTypesGeneric(types :_*)
  setSkipFirstLineAsHeader(true)

  override protected def fillRecord(reuse: MobilePage, parsedValues: Array[AnyRef]): MobilePage = {
    val builder = MobilePage.newBuilder()
    for (i <- parsedValues.indices) {
      builder.setField(fields.get(i), parsedValues(i))
    }
    builder.build
  }
}
