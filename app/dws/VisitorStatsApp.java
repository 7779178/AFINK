package com.ls.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ls.bean.VisitorStats;
import com.ls.utils.ClickHouseUtil;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

// TODO: 2023/7/25 访客主题
//数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)
// -> FlinkApp -> ClickHouse
//进  程：MockLog -> Nginx -> Logger -> Kafka(ZK) -> BaseLogApp -> Kafka -> uvApp userJumpApp -> Kafka
// -> VisitorStatsApp -> ClickHouse
public class VisitorStatsApp {

    public static void main(String[] args) throws Exception {

        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
//        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 2 读取3个主题的数据
        String groupId = "visitor_stats_app_0923";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtil.getKafkaSource(groupId, pageViewSourceTopic));
        DataStreamSource<String> uvSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(groupId, uniqueVisitSourceTopic));
        DataStreamSource<String> userJumpSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(groupId, userJumpDetailSourceTopic));

        //TODO 3 取出dwd_page_log主题数据过滤出进入页面数据
        //{"common":{"ar":"440000","ba":"Xiaomi","ch":"xiaomi","is_new":"1","md":"Xiaomi 9","mid":"mid_1","os":"Android 9.0","uid":"26","vc":"v2.1.134"},"page":{"during_time":4385,"item":"1,6","item_type":"sku_ids","last_page_id":"cart","page_id":"trade"},"ts":1615862194000}
        SingleOutputStreamOperator<String> scSourceDS = pageLogDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");
                //如果lastPage == null || lastPage.length() <= 0，之前没有页面id，证明用户第一次登录业面。
                return lastPage == null || lastPage.length() <= 0;
            }
        });

        //TODO 4 将4个流统一数据格式
        //4.1 格式化PV和访问时长
        SingleOutputStreamOperator<VisitorStats> pvAndTimeDS = pageLogDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    0L,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        //4.2 格式化UV
        SingleOutputStreamOperator<VisitorStats> uvDS = uvSourceDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //4.3 格式化跳出数据
        SingleOutputStreamOperator<VisitorStats> userJumpDS = userJumpSourceDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //4.4 格式化进入页面数据
        SingleOutputStreamOperator<VisitorStats> svDS = scSourceDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    1L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //TODO 5 union4个流的数据
        DataStream<VisitorStats> unionDS = pvAndTimeDS.union(uvDS, svDS, userJumpDS);

        //TODO 6 开窗聚合操作
        //6.1 提取数据中的时间戳生成WaterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //6.2 分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWmDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getVc(),
                        value.getCh(),
                        value.getAr(),
                        value.getIs_new());
            }
        });

        //6.3 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //6.4 聚合
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                //取出窗口的开始和结束时间
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();

                //取出数据
                VisitorStats visitorStats = iterable.iterator().next();

                //设置窗口的开始和结束时间
                visitorStats.setStt(sdf.format(start));
                visitorStats.setEdt(sdf.format(end));

                //输出数据
                collector.collect(visitorStats);
            }
        });

        //TODO 7 将数据写入ClickHouse
        result.print(">>>>>>>>>>>");
        result.addSink(ClickHouseUtil.<VisitorStats>getClickHouseSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8 启动任务
        env.execute();

    }

}
