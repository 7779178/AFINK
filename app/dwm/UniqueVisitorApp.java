package com.ls.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

public class UniqueVisitorApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 读取 dwd_page_log 主题数据创建流
        String groupId = "unique_visitor_app_group";
        String topic = "dwd_page_log";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(groupId, topic);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 3 将每行数据转换JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4 按照Mid分组
        KeyedStream<JSONObject, JSONObject> jsonObjWithMidDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common"));

        //TODO 5 使用状态编程的方式对数据做过滤  富函数、Process
        SingleOutputStreamOperator<JSONObject> dauDS = jsonObjWithMidDS.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;

            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-date", String.class);
                StateTtlConfig stateTtlConfig = StateTtlConfig
                        .newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                valueState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPasgeId = jsonObject.getJSONObject("page").getString("last_page_id");

                if (lastPasgeId == null || lastPasgeId.length() <= 0) {
                    //取出状态数据
                    String visitDate = valueState.value();

                    String date = sdf.format(jsonObject.getLong("ts"));

                    if (visitDate == null || visitDate.length() <= 0 || !visitDate.equals(date)) {
                        //更新状态数据
                        valueState.update(date);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        //TODO 6 将数据写入DWM层主题
        dauDS.print(">>>>>>>>>");
        dauDS.map(JSONAware :: toJSONString)
                        .addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));


        env.execute();
    }
}
