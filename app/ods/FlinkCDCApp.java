package com.ls.app.ods;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ls.app.Fucn.MyDeserializationSchemaFunction;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> result = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .databaseList("gmall2021") // set captured database
                .username("root")
                .password("123456")
                .deserializer(new MyDeserializationSchemaFunction()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.latest())
                .build();
        // enable checkpoint
        DataStreamSource<String> mysqlDS = env.addSource(result);

        String topic ="ods_base_db";
        mysqlDS.addSink(MyKafkaUtil.getKafkaSink(topic));
        mysqlDS.print(">>>>>>>");
        //4.启动任务
        env.execute();

    }
}
