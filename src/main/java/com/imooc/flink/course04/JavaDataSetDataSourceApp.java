package com.imooc.flink.course04;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


import java.util.ArrayList;
import java.util.List;

public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //fromCollection(env);

        csvFile(env);


    }

    public static void csvFile(ExecutionEnvironment env) throws Exception{
        String filePath = "file:///Users/micheal/Documents/input/people.csv";

        env.readCsvFile(filePath).ignoreFirstLine().includeFields("100").types(String.class).print();

    }

    public static void textFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///Users/micheal/Documents/input/hello_world.txt";
        env.readTextFile(filePath).print();
        System.out.println("-----分割线-----");

        filePath = "file:///Users/micheal/Documents/input";
        env.readTextFile(filePath).print();

    }

    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for(int i = 1; i <=10;i++){
            list.add(i);
        }

        env.fromCollection(list).print();
    }
}
