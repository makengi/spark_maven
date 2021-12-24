//package com.imr.spark.listener;
//
//import org.apache.spark.launcher.SparkAppHandle;
//import org.apache.spark.launcher.SparkAppHandle.Listener;
//import org.apache.spark.launcher.SparkAppHandle.State;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import static java.lang.String.format;
//import static java.util.Objects.nonNull;
//
//public class LauncherStateListener implements Listener {
//
//    private static Logger Log = LoggerFactory.getLogger(LauncherStateListener.class);
//
//    @Override
//    public void stateChanged(SparkAppHandle handle) {
//        final String sparkApplicationId = handle.getAppId();
//
//        final State appState = handle.getState();
//
//        if(nonNull(sparkApplicationId)){
//            Log.info(format("Spark job with app id: %s- state change to :%s",sparkApplicationId,appState));
//        }else{
//            Log.info(format("Spark job's state changed to :%s",appState));
//        }
//    }
//
//    @Override
//    public void infoChanged(SparkAppHandle handle) { }
//}
