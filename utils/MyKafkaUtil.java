package com.ls.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    //准备配置信息
    private static Properties properties = new Properties();

    //指定DWD事实数据默认主题
    private static final String DWD_DEFAULT_TOPIC = "dwd_default_topic";

    //Kafka连接参数
    private static final String KAFKA_SERVER = "hadoop101:9092,hadoop102:9092,hadoop103:9092";

    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    }

    /**
     * 获取Kafka生产者
     *
     * @param topic 写入Kafka的主题名
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {

        //创建Kafka生产者对象并返回
        return new FlinkKafkaProducer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSchema) {

        //增加Kafka事务超时时间配置
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "");

        //创建Kafka生产者对象并返回
        return new FlinkKafkaProducer<T>(DWD_DEFAULT_TOPIC,
                kafkaSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 获取Kafka的消费者
     *
     * @param groupId 消费者组
     * @param topic   消费的主题
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId, String topic) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset'  ";
    }

}