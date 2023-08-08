package com.ls.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 从Kafka dwd_page_log 主题读取数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_app_group";
        String sinkTopic = "dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(groupId, sourceTopic);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 3 将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                });
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(watermarkStrategy);

        //TODO 4 按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //TODO 5 定义模式序列
        //{"common":{"ar":"440000","ba":"Xiaomi","ch":"xiaomi","is_new":"1","md":"Xiaomi 9","mid":"mid_1","os":"Android 9.0","uid":"26","vc":"v2.1.134"},"page":{"during_time":4385,"item":"1,6","item_type":"sku_ids","last_page_id":"cart","page_id":"trade"},"ts":1615862194000}
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null && lastPageId.length() <= 0;
                    }
                }).followedBy("follow").where(new SimpleCondition<JSONObject>() {

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId != null && lastPageId.length() > 0;
                    }
                }).within(Time.seconds(10));

        //TODO 6 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7 提取超时事件
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("Timeout") {
        };
        SingleOutputStreamOperator<Object> result = patternStream.select(outputTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        List<JSONObject> begin = map.get("begin");
                        return begin.get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, Object>() {
                    @Override
                    public Object select(Map<String, List<JSONObject>> map) throws Exception {
                        //不需要业务逻辑,因为匹配上的数据反而是不要的数据
                        return null;
                    }
                });

        //TODO 8 提取侧输出流数据写入Kafka主题
        DataStream<JSONObject> userJumpDetailDS = result.getSideOutput(outputTag);
        userJumpDetailDS.print(">>>>>>");
        userJumpDetailDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //TODO 9 启动任务
        env.execute();

    }
}
