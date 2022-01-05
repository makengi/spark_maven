package com.imr.spark.config;

import lombok.ToString;
import org.apache.spark.SparkConf;

@ToString
public abstract class AbstractSparkConfig {

    private String master;
    private String appName;
    private String encoding;

    public AbstractSparkConfig(){

    }

    public AbstractSparkConfig(String master){
        this(master, "application", "UTF-8");
    }

    public AbstractSparkConfig(String master, String appName,String encoding){
        this.master = master;
        this.appName = appName;
        this.encoding = encoding;
    }


    public SparkConf compose(){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(this.master);
        sparkConf.setAppName(this.appName);
        sparkConf.set("encoding",this.encoding);
        return sparkConf;
    }

}
