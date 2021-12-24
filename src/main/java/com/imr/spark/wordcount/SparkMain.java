package com.imr.spark.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Arrays;

public class SparkMain {

    private static Logger Log = LoggerFactory.getLogger(SparkMain.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[8]")
                .setAppName("cctv");


        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        URL url = SparkMain.class.getResource("/files/cctv_data.csv");
        //Load DataSets

        JavaRDD<String> videos = sparkContext.textFile(url.getPath());


        //TransFormations

        JavaRDD<String> titles = videos
                .map(SparkMain::exactTitle)
                .filter(StringUtils::isNotBlank);

        JavaRDD<String> words = titles.flatMap((title) -> Arrays.asList(title.toLowerCase()).iterator());
        words.foreach(word -> Log.info(word.toString()));

    }

    public static String exactTitle(String textLine){
        try{
            return textLine.split(SplitEnum.COMMA_DELIMITER.getName())[0];
        }catch (Exception e){
            e.printStackTrace();
            return "";

        }
    }
}
