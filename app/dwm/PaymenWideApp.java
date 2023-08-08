package com.ls.app.dwm;

import com.alibaba.fastjson.JSON;
import com.ls.bean.OrderWide;
import com.ls.bean.PaymentInfo;
import com.ls.bean.PaymentWide;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class PaymenWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

            String groupId="payment_wide_group";
            String paymentInfoSourceTopic ="dwd_pyment";
            String orderWideSourceTopic = "dwm_order_wide";
            String PAYmentWideSinkTopic = "dwm_payment_wide";

        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(groupId, paymentInfoSourceTopic);
        DataStreamSource<String> paymentInfoStrDS = env.addSource(paymentInfoSource);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(groupId, orderWideSourceTopic);
        DataStreamSource<String> orderWideStrDS = env.addSource(orderWideSource);

        //TODO 3 转换为JavaBean对象并提取时间戳生成WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(data -> JSON.parseObject(data, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                try {
                                    return sdf.parse(paymentInfo.getCreate_time()).getTime();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    return 0L;
                                }

                            }
                        }));
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(data -> JSON.parseObject(data, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide orderWide, long l) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        try {
                            return sdf.parse(orderWide.getCreate_time()).getTime();
                        } catch (Exception e) {
                            e.printStackTrace();
                            return 0L;
                        }
                    }
                }));
        //TODO 4 双流JOIN并处理JOIN结果
        SingleOutputStreamOperator<Object> result = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, Object>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, Object>.Context context, Collector<Object> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });
        //TODO 5 将数据写入Kafka
        result.print();
        result.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(PAYmentWideSinkTopic));

        //TODO 6 启动任务
        env.execute();

    }
}
