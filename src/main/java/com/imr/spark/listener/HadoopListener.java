package com.imr.spark.listener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class HadoopListener implements WatchAble{

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopListener.class);

    private HdfsAdmin hdfsAdmin;
    private DFSInotifyEventInputStream eventInputStream;

    public HadoopListener(String path) {
        try{
            this.hdfsAdmin = new HdfsAdmin(URI.create(path),new Configuration());
            this.eventInputStream = this.hdfsAdmin.getInotifyEventStream();
        }catch (IOException e){}
    }

    @Override
    public void watch() throws IOException, MissingEventsException, InterruptedException {
        /**
         *  watch Hdfs
         */
        while(true){
            EventBatch eventBatch = this.eventInputStream.take();
            for(Event event: eventBatch.getEvents()){
                LOGGER.info("event type: {}",event.getEventType());
            }
        }

    }
}
