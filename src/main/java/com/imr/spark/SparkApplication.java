package com.imr.spark;

import com.imr.spark.mapreduce.CcTvMapper;
import com.imr.spark.mapreduce.CctvReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class SparkApplication {

    private static Logger Log = LoggerFactory.getLogger(SparkApplication.class);

    public void proc1() throws IOException {


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://192.168.0.19:7077");
        sparkConf.setAppName("com.imr.ji spark applications");
        sparkConf.set("encoding", "UTF-8");
        sparkConf.setJars(new String[]{System.getProperty("user.dir") + "/spark/files/java_spark.jar"});
        sparkConf.set("class", "com.imr.spark.SparkApplication");
        sparkConf.set("driver-memory", "1024M");
        sparkConf.set("deploy-mode", "cluster");
        sparkConf.set("executor-memory", "1024M");


        SparkSession session = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();


//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Dataset<Row> rows = session.read().csv("hdfs://192.168.0.19:9000/cctv_data.csv");


        rows.select("_c0", "_c4").show();

        Column column = rows.col("_c4").cast("integer");
        Column column1 = rows.col("_c0");

        Column dateColumn = rows.col("_c13").$greater(535000);


        Dataset<Row> groupByRows =
                rows.select(column1, column)
                        .where(dateColumn)
                        .groupBy("_c0")
                        .sum("_c4");

        groupByRows.show();
        rows.show();
//        System.out.println(rows.encoder());
//        String textString = "한글";
//        Log.info(getStringToHex(textString));
//        Log.info(textString);
//        rows.show();
// 
//        rows.select("관리기관명").collectAsList()
//                .forEach(System.out::println);
}
    public static void main(String[] args) throws Exception {
        SparkApplication application = new SparkApplication();
        application.proc1();

    }
}
