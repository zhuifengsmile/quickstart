package org.myorg.quickstart.state

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object TestState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // test stateful map
    env.generateSequence(0, 10).setParallelism(1)
      .map { v => (1, v) }.setParallelism(1)
      .keyBy(_._1)
      .mapWithState((in, count: Option[Long]) =>
        count match {
//          case Some(c) => (in._2 - c, Some(c + 1))
//          case None => (in._2, Some(1L))
          case Some(c) => ( (in._1, c), Some(c + in._2) )
          case None => ( (in._1, 0), Some(in._2) )
        }).setParallelism(1)
      .print()

//      .addSink(new RichSinkFunction[Long]() {
//        var allZero = true
//        override def invoke(in: Long) = {
//          if (in != 0) allZero = false
//        }
//        override def close() = {
//          assert(allZero)
//        }
//      })
    env.execute()
  }
}
