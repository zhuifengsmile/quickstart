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

import org.apache.flink.streaming.api.scala._

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingIterateJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */
    val someIntegers: DataStream[Long] = env.generateSequence(0, 4).setParallelism(1)

    val iteratedStream = someIntegers.iterate(
      iteration => {
        val minusOne = iteration.map( v => v - 1).setParallelism(1)
        val stillGreaterThanZero = minusOne.filter (_ > 0).setParallelism(1)
        val lessThanZero = minusOne.filter(_ >= 1).setParallelism(1)
        (stillGreaterThanZero, lessThanZero)
      }
    ).setParallelism(1)

    iteratedStream.print().setParallelism(1)
//    someIntegers.print().setParallelism(1)


    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
