package com.imr.spark;

import com.imr.spark.hbase.FunctionGroup;
import com.imr.spark.model.KafkaMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;


public class SparkApplication {

    private static Logger Log = LoggerFactory.getLogger(SparkApplication.class);
    private static ThreadLocal<Connection> connHolder= new ThreadLocal<Connection>();
    private static int rowKey = 1;

    public static void proc1() throws IOException, InterruptedException {

//        String master = "spark://192.168.0.19:7077";
        String master = "local[*]";
//        String driver= "spark.driver.host", "localhost"
        String appName = "cctv application";
        String encoding = "UTF-8";

//        AbstractSparkConfig config = new ClusterSystemConfig(master, appName, encoding);
//        SparkConf sparkConf = config.compose();

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("cctv application");
        sparkConf.set("spark.driver.host","localhost");
        sparkConf.set("encoding", "UTF-8");
//        sparkConf.setMaster("local[*]");

//        sparkConf.setJars("/home/imr/spark/files/java_spark.jar");
//        sparkConf.set("class", "com.imr.spark.SparkApplication");
//        sparkConf.set("driver-memory", "1024M");
//        sparkConf.set("deploy-mode", "cluster");
//        sparkConf.set("executor-memory", "1024M");

        Log.info("start spark application");

//        SparkSession spark = SparkSession
//                            .builder()
//                            .config(sparkConf)
//                            .getOrCreate();




//        Dataset<Row> subscribeResult = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "master:9092")
//                .option("subscribe", "SmartCar-Topic").load();
//
//
//        subscribeResult.show();

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(jsc,Durations.milliseconds(3000));


        Map<String, Object> kafkaConnectOption = new HashMap<>();
        kafkaConnectOption.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        kafkaConnectOption.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConnectOption.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConnectOption.put("enable.auto.commit", "false");
        kafkaConnectOption.put("group.id","spark-stream-kafka");


        Collection<String> topics = Arrays.asList("SmartCar-Topic"); // ""안에 kafka topic명 입력

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                javaStreamingContext
                , LocationStrategies.PreferConsistent()
                , ConsumerStrategies.<String, String>Subscribe(topics, kafkaConnectOption));



        /*해석해 보자
                record의 키와 벨류를 리스트로 만들어서 반환해주는거 뿐?
                        첫번째 인자는 파라미터 ConsumerRecord<String,String>
                두번째 세번째는 튜플의 타입
         */


        PairFunction<ConsumerRecord<String,String>,String,String> pairFunction = (record)->{
            return new Tuple2<String,String>(record.key(),record.value());
        };

        JavaPairDStream<String,String> stringPairStream  = stream.mapToPair(pairFunction);

        Function<Tuple2<String,String>, KafkaMessage> function = (tuple)->{
            Log.warn("tuple_1: "+tuple._1);
            Log.warn("tuple_2: "+tuple._2);
            return KafkaMessage.builder().first(tuple._1).second(tuple._2).build();
    };

    Function<KafkaMessage,KafkaMessage> assignKeyFunction = (targetObject)->{
        String value = targetObject.getSecond();
        String [] splitedValues = value.split(",");
        return KafkaMessage.builder().first(splitedValues[0]).second(targetObject.toString()).build();
    };


        List<String> stringList = new ArrayList<>();

        JavaDStream<KafkaMessage> kafkaMessageJavaDStream =
                stringPairStream.map(function)
                .map(assignKeyFunction);

        kafkaMessageJavaDStream.print();

        stream.map(ConsumerRecord::value).print();
        String tableName = "SmartCarInfo";
        streamBulkPut(jsc,tableName, kafkaMessageJavaDStream);


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    /*    JavaInputDStream<ConsumerRecord<String,String>> stream = KafkaUtils.createDirectStream(
                ssc,LocationStrategies.PreferConsistent(),ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams));

    */


//        Dataset<Row> rows = session.read().csv("hdfs://192.168.0.19:8020/cctv_data.csv");
//
//
//        rows.select("_c0", "_c4").show();
//
//        Column column = rows.col("_c4").cast("integer");
//        Column column1 = rows.col("_c0");
//
//        Column dateColumn = rows.col("_c13").$greater(535000);
//        Column location = rows.col("_c2").startsWith("경상남도");
//
//
//        Dataset<Row> groupByRows =
//                rows.select(column1, column)
//                        .where(dateColumn)
//                        .where(location)
//                        .groupBy("_c0")
//                        .sum("_c4");
//
//        groupByRows.show();
//
//        rows.show();
//        session.close();

    }


    /**
     * Put JavaDStream<KafkaMessageStream> putDStream
     * "rowKey,columnFamily,columnKey,value"
     */

    public static void streamBulkPut(JavaSparkContext jsc, String tableName, JavaDStream<KafkaMessage> kafkaMessageJavaDStream) throws IOException {

        Connection connection = connHolder.get();

        if (connection == null){
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum","master,slave01,slave02");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
            connHolder.set(connection);
        }

        Admin admin = connection.getAdmin();


        if(!admin.tableExists(TableName.valueOf(tableName))) {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(tableName));

            /*
                테이블이 없을때 테이블 생성하고 FamilyColumn 지정
             */
            ColumnFamilyDescriptorBuilder driverInfoColumnFamily =ColumnFamilyDescriptorBuilder.
                                        newBuilder(Bytes.toBytes("driverInfo"));

//            ColumnFamilyDescriptorBuilder etcInfoColumnFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("otherInfo"));

            tableDescriptorBuilder.setColumnFamily(driverInfoColumnFamily.build());
//            tableDescriptorBuilder.setColumnFamily(etcInfoColumnFamily.build());

            /*
                테이블 생성
             */
            admin.createTable(tableDescriptorBuilder.build());

        }else{

            Configuration conf = HBaseConfiguration.create();
            conf.addResource(new Path("/home/imr/hadoop/etc/hadoop", "hbase-site.xml"));
            conf.addResource(new Path("/home/imr/hadoop/etc/hadoop", "core-site.xml"));
            conf.addResource(new Path("/home/imr/hadoop/etc/hadoop", "hdfs-site.xml"));
//
//        conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"),"hbase-site.xml"));
//        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
//        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));

            Log.warn("Configuration Directory: {} ", conf.toString());
            JavaHBaseContext hBaseContext = new JavaHBaseContext(jsc, conf);
            hBaseContext.streamBulkPut(kafkaMessageJavaDStream, TableName.valueOf(tableName), new FunctionGroup.PutFunction());
        }

    }




    public static void main(String[] args) throws Exception {
        SparkApplication application = new SparkApplication();
        SparkApplication.proc1();

    }
}

