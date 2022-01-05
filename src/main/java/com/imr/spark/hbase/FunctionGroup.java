package com.imr.spark.hbase;

import com.imr.spark.model.KafkaMessage;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;

public class FunctionGroup {


    private static int rowKey=1;

    public static class PutFunction implements Function<KafkaMessage, Put> {

        String tmpColumnFamily = "driverInfo";

        private static final long serialVersionUID = 8038080193924048202L;
        @Override
        public Put call(KafkaMessage inputTuple) throws Exception {
            String[] args = inputTuple.getSecond().split(",");
            Put put =  new Put(Bytes.toBytes(rowKey++))
                    .addColumn(Bytes.toBytes(tmpColumnFamily),
                            Bytes.toBytes("kafka"),
                            Bytes.toBytes(args[3]));
            return put;
        }
    }


}
