package com.ls.app.dws;

import com.ls.bean.ProvinceStats;
import com.ls.utils.ClickHouseUtil;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//数据流: Web/App -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd/dim) ->
//       FlinkApp -> Kafka(dwm) -> FlinkApp -> ClickHouse
//进程:           MockDB                 -> Mysql -> FlinkCDCApp -> Kafka(ZK) -> BaseDBApp -> Kafka(Phoenix)          -> OrderWideApp(Redis) -> Kafka -> ProvinceStatsApp -> ClickHouse
public class ProvinceStatsApp {

    public static void main(String[] args) throws Exception {

        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //Blink扩展了很多功能，所以使用blink
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                //使用blink当中流的模式
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2 使用TableEnv读取Kafka dwm_order_wide主题的数据  DDL
        String groupId = "province_stats_app_0923";
        String topic = "dwm_order_wide";
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (" +
                " province_id BIGINT, " +
                " province_name STRING," +
                " province_area_code STRING," +
                " province_iso_code STRING," +
                " province_3166_2_code STRING," +
                " order_id STRING," +
                " total_amount DOUBLE," +
                " create_time STRING," +
                " rowtime AS TO_TIMESTAMP(create_time)," +
                " WATERMARK FOR rowtime AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        //TODO 3 执行查询 开窗、分组、聚合
        Table table = tableEnv.sqlQuery("select" +
                "    date_format(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "    date_format(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "    province_id," +
                "    province_name," +
                "    province_area_code," +
                "    province_iso_code," +
                "    province_3166_2_code," +
                "    count(*) order_count," +
                "    sum(total_amount) order_amount," +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from ORDER_WIDE " +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND)," +
                "    province_id," +
                "    province_name," +
                "    province_area_code," +
                "    province_iso_code," +
                "    province_3166_2_code");

        //TODO 4 将动态表转换为流
        DataStream<ProvinceStats> resultDS = tableEnv.toAppendStream(table, ProvinceStats.class);

        //TODO 5 写入ClickHouse
        resultDS.print();
        resultDS.addSink(ClickHouseUtil.getClickHouseSink("insert into province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6 启动任务
        env.execute();

    }

}
