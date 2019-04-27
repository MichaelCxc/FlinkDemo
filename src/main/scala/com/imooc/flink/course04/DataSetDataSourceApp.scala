package com.imooc.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetDataSourceApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // fromCollection(env)


    //textFile(env)

    csvFile(env)
  }

  case class MyCaseClass(name:String, age:Int)


  def csvFile(env: ExecutionEnvironment): Unit = {
    //val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val filePath = "file:///Users/micheal/Documents/input/people.csv"
    //env.readCsvFile[(String,Int,String)](csvPath,ignoreFirstLine=true).print()

    //env.readCsvFile[(String,Int)](csvPath,ignoreFirstLine=true,includedFields = Array(0,1)).print()


    //env.readCsvFile[MyCaseClass](filePath, ignoreFirstLine = true, includedFields = Array(0,1)).print()

    env.readCsvFile[Person](filePath, ignoreFirstLine = true, pojoFields = Array("name","age","work")).print()  }


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
