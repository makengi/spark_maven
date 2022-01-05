package com.imr.spark.listener;

import org.apache.hadoop.hdfs.inotify.MissingEventsException;

import java.io.IOException;

public interface WatchAble {
    public void watch() throws IOException, MissingEventsException, InterruptedException;
}
