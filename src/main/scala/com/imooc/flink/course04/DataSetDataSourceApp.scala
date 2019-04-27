package com.imooc.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetDataSourceApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // fromCollection(env)


    textFile(env)


  }


  def textFile(env: ExecutionEnvironment): Unit = {
    val filePath = "file:///Users/micheal/Documents/input/hello_world.txt"
    env.readTextFile(filePath).print()
  }



  def fromCollection(env: ExecutionEnvironment): Unit = {

    import org.apache.flink.api.scala._

    val data = 1 to 10
    env.fromCollection(data).print()

  }
}
