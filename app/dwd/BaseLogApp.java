package com.ls.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.trogdor.workload.PayloadGenerator;

import javax.xml.bind.ValidationEvent;
import java.text.SimpleDateFormat;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //并行度设置应该与Kafka主题的分区数一致

        //TODO 2 读取Kafka ods_base_log 主题数据创建流
        String groupId="base_log_app_group";
        String topic="ods_base-log";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(groupId, topic);
        DataStreamSource<String> kafkDS = env.addSource(kafkaSource);

        //TODO 3 新老用户校验 (分组 状态编程 富函数)
        OutputTag<String> outputTag = new OutputTag<String>("dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDDS = kafkDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(outputTag, s);
                }
            }
        });
        KeyedStream<JSONObject, String> jsonObjWithMidKeyDS = jsonObjDDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjWithMidKeyDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> firstVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visit-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");

                //如果标记为1,则进行校验
                if ("1".equals(isNew)) {

                    //获取状态数据
                    String firstVisitDate = firstVisitDateState.value();
                    //判断状态是否为null
                    if (firstVisitDate != null) {
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    } else {
                        Long ts = jsonObject.getLong("ts");
                        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-mm-dd");
                        firstVisitDateState.update(dateFormat.format(ts));
                    }
                }

                return jsonObject;
            }
        });
        //打印测试
        jsonObjWithNewFlagDS.print(">>>>>>>");
        jsonObjDDS.getSideOutput(outputTag).print("脏数据>>>>>");

        //TODO 4 使用侧输出流对原始数据流进行切分
        //页面日志写入主流,启动日志写入侧输出流,曝光数据写入侧输出流
        OutputTag<String> startoutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayoutputTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pagelogDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //获取Start数据
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //为页面数据
                    context.output(startoutputTag, jsonObject.toJSONString());
                } else {
                    //为页面数据
                    collector.collect(jsonObject.toJSONString());

                    //获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            //获取单条曝光数据
                            JSONObject displayjsonObject = displays.getJSONObject(i);
                            //获取数据中的页面ID
                            String pageID = jsonObject.getJSONObject("page").getString("page_id");
                            //将页面ID插入进曝光数据中
                            displayjsonObject.put("page_id", pageID);
                            //将曝光数据写入侧输出流
                            context.output(displayoutputTag, displayjsonObject.toJSONString());
                        }
                    }
                }
            }
        });
        //TODO 5 将得到的不同的流输出到不同的DWD层主题
        pagelogDS.print("Page>>>>>>>>>");
        pagelogDS.getSideOutput(startoutputTag).print("start>>>>>");
        pagelogDS.getSideOutput(displayoutputTag).print("display>>>>>");
        pagelogDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        pagelogDS.getSideOutput(startoutputTag).addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 6 启动任务
        env.execute();

    }
}
