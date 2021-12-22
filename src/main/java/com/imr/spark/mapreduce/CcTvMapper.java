package com.imr.spark.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CcTvMapper extends Mapper<Object, Text,Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static Logger Logger = LoggerFactory.getLogger(CcTvMapper.class);


    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 관리 기관명 추출
        String[] strs = value.toString().split("\t");
        Logger.info("strs[0] value: {}",strs[0]);
        word.set(strs[0]);

        context.write(word, one);
    }
}
