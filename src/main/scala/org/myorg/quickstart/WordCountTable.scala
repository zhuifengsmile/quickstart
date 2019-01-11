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

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

/**
  * Simple example for demonstrating the use of the Table API for a Word Count in Scala.
  *
  * This example shows how to:
  *  - Convert DataSets to Tables
  *  - Apply group, aggregate, select, and filter operations
  *
  */
object WordCountTable {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


//    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
//    val expr = input.toTable(tEnv)
//    val result = expr
//      .groupBy('word)
//      .select('word, 'frequency.sum as 'frequency)
//      .filter('frequency === 2)
//      .toDataSet[WC]

//    val input = env.fromCollection(Seq(AWS(1, 1), AWS(1, 2), AWS(3, 10), AWS(3, 2),AWS(6, 3))).setParallelism(1)
    val input = env.fromCollection(Seq(AWS(1, 1), AWS(2, 1), AWS(2, 2), AWS(3, 3),AWS(3, 5)))

    tEnv.registerDataStream("WordCount", input, 'word, 'frequency)
    tEnv.registerFunction("wAvg", new DistinctAggFunction())

    // union the two tables
    val resulta = tEnv
      .sqlQuery("SELECT word,wAvg(frequency) as frequency FROM WordCount GROUP BY word")
//    val result = tEnv.sqlQuery("SELECT word, sum(frequency) FROM " + resulta + " GROUP BY word")
    val resultb = tEnv.sqlQuery("SELECT sum(frequency) FROM " + resulta)
//    val result = tEnv.sqlQuery("SELECT frequency, count(word) FROM " + resulta + " GROUP BY frequency")

//    result.toAppendStream[AWS].print()
    resultb.toRetractStream[Long].print().setParallelism(1)
//    result.toRetractStream[LWS].setParallelism(1).print().setParallelism(1)
//    result.toRetractStream[LWS].setParallelism(1).print().setParallelism(2)
//    result.toRetractStream[LWS].print().setParallelism(1)

    env.execute()


//    val expr = input.toTable(tEnv)
//    val result = expr
//      .groupBy('word)
////      .select('word, 'frequency.sum as 'frequency)
//      .select('word, 'frequency.sum as 'frequency)
////      .filter('frequency === 2)
//      .toDataSet[AWS]
//
//    result.print()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WC(word: String, frequency: Long)
  case class AWS(word: Int, frequency: Long)
  case class LWS(word: Long, frequency: Long)

}
