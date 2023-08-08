package com.ls.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ls.app.Fucn.DimSink;
import com.ls.app.Fucn.MyDeserializationSchemaFunction;
import com.ls.app.Fucn.TableProcessFunction;
import com.ls.bean.TableProcess;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//TODO 2 读取Kafka ods_base_db 主题数据创建流
        String groupId="base_db_app_group";
        String topic="ods_base_db";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(groupId, topic);
        DataStreamSource<String> streamSource = env.addSource(kafkaSource);
        //TODO 3 过滤空值数据(主流)
        SingleOutputStreamOperator<JSONObject> jsonDS = streamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("发现脏数据...." + s);
                }
            }
        });
        SingleOutputStreamOperator<JSONObject> fliterDS = jsonDS.filter(jsonObj -> {
            String data = jsonObj.getString("data");
            return data != null && data.length() > 0;
        });

        //TODO 4 使用FlinkCDC读取配置表形成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop-single")
                .port(3306)
                .databaseList("gmall2021-realtime") // set captured database
                .tableList("gmall2021-realtime.table_process") // set captured table
                .username("root")
                .password("root")
                .deserializer(new MyDeserializationSchemaFunction()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);
        //为下面的广播流准备状态描述器
        MapStateDescriptor<String, TableProcess> stateDescriptor = new MapStateDescriptor<>("table-process", String.class, TableProcess.class);
        //通过源码发现使用广播流需要上面的状态描述器
        BroadcastStream<String> broadcast = mysqlDS.broadcast(stateDescriptor);

        //TODO 5 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = fliterDS.connect(broadcast);
        //TODO 6 分流
        OutputTag<JSONObject> hbaseOutputTag = new OutputTag<>("hbase");
        SingleOutputStreamOperator<JSONObject> presultDS = connectedStream.process(new TableProcessFunction(hbaseOutputTag, stateDescriptor));

        ///TODO 7 将分好的流写入Phoenix表(维度数据)或者Kafka主题(事实数据)
        fliterDS.print("主流的原始数据");
        presultDS.print("kafuka");
        presultDS.getSideOutput(hbaseOutputTag).print("Hbase>>>>>>>>");

        //将数据写入Phoenix
        presultDS.getSideOutput(hbaseOutputTag).addSink(new DimSink());

         //将数据写入Kafka
        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {

            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化kafka数据");
            }

            //element:{"database":"","table":"","type":"","data":{"id":"11"...},"sinkTable":"dwd_xxx_xxx"}

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("data").getBytes());
            }
        });
        presultDS.addSink(kafkaSinkBySchema);
        //TODO 8 启动任务
        env.execute();
    }
}
