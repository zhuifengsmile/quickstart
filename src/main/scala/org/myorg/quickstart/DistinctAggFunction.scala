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
package org.myorg.quickstart

import java.lang.{Iterable => JIterable, Long => JLong}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for count aggregate function */
class DistinctAggAccumulator extends JTuple1[Long] {
  f0 = 0L //count
}

/**
  * built-in count aggregate function
  */
class DistinctAggFunction
  extends AggregateFunction[JLong, DistinctAggAccumulator] {

  // process argument is optimized by Calcite.
  // For instance count(42) or count(*) will be optimized to count().
  def retract(acc: DistinctAggAccumulator, value: JLong): Unit = {
    if (value != null) {
      acc.f0 = value
    }
  }

  def accumulate(acc: DistinctAggAccumulator, value: JLong): Unit = {
    if (value != null) {
      acc.f0 = value
    }
  }

  override def getValue(acc: DistinctAggAccumulator): JLong = {
    acc.f0
  }

  def merge(acc: DistinctAggAccumulator, its: JIterable[DistinctAggAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0 += iter.next().f0
    }
  }

  override def createAccumulator(): DistinctAggAccumulator = {
    new DistinctAggAccumulator
  }

  def resetAccumulator(acc: DistinctAggAccumulator): Unit = {
    acc.f0 = 0L
  }

  override def getAccumulatorType: TypeInformation[DistinctAggAccumulator] = {
    new TupleTypeInfo(classOf[DistinctAggAccumulator], BasicTypeInfo.LONG_TYPE_INFO)
  }

  override def getResultType: TypeInformation[JLong] =
    BasicTypeInfo.LONG_TYPE_INFO
}
