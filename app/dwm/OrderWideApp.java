package com.ls.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ls.app.Fucn.AsyncDimFunction;
import com.ls.bean.OrderDetail;
import com.ls.bean.OrderInfo;
import com.ls.bean.OrderWide;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 读取Kafka dwd_order_info dwd_order_detail主题的数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_app_group";
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(groupId, orderInfoSourceTopic);
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(groupId, orderDetailSourceTopic);
        DataStreamSource<String> orderInfoJsonStrDS = env.addSource(orderInfoSource);
        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);

        //        orderInfoJsonStrDS.print("原始OrderInfo>>>>>>>>>>");
//        orderDetailJsonStrDS.print("原始OrderDetail>>>>>>>>>>");

        //TODO 3 将2个流转换为JavaBean并提取数据中的时间戳生成WaterMark
        //操作orderInfo表
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonStrDS.map(data -> {
            OrderInfo orderInfo = JSON.parseObject(data, OrderInfo.class);

            //补充其中的时间字段
            String create_time = orderInfo.getCreate_time();

            System.out.println(create_time);

            String[] dateHourArr = create_time.split(" ");
            orderInfo.setCreate_date(dateHourArr[0]);
            orderInfo.setCreate_hour(dateHourArr[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                }));
        //操作orderDetail表
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonStrDS.map(data -> {
            OrderDetail ordetailInfo = JSON.parseObject(data, OrderDetail.class);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            ordetailInfo.setCreate_ts(sdf.parse(ordetailInfo.getCreate_time()).getTime());
            return ordetailInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                        return orderDetail.getCreate_ts();
                    }
                }));

        //TODO 4 双流JOIN
        //利用id进行join，所以要获取两张表的id
        KeyedStream.IntervalJoined<OrderInfo, OrderDetail, Long> joinedDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                //给事件时间及范围
                .inEventTime()
                //给时间范围，一个上界，一个下界
                .between(Time.seconds(-5), Time.seconds(5));

        SingleOutputStreamOperator<OrderWide> orderWideDS = joinedDS.process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
            @Override
            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> collector) throws Exception {
                //创建javaBean，将返回值所有类型封装起来
                collector.collect(new OrderWide(orderInfo, orderDetail));
            }
        });
        orderWideDS.print("orderwide>>>>>>>>");

        //TODO 5 查询Phoenix,补全维度信息
        //5.1 关联用户维度
        //unorderedWait表示数据不需要关注顺序,处理完立即发送
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimJSON) {
                        if (dimJSON != null) {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            String birthday = dimJSON.getString("BIRTHDAY");
                            Long age = 0L;
                            try {
                                age = (System.currentTimeMillis() - sdf.parse(birthday).getTime());

                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            orderWide.setUser_age(age.intValue());
                            String gender = dimJSON.getString("GENDER");
                            orderWide.setUser_gender(gender);
                        }
                    }
                },
                100,
                TimeUnit.SECONDS);

        orderWideWithUserDS.print("User>>>>>>>>>>>>");

        //TODO 5 查询Phoenix,补全维度信息
        //5.2 关联省份维度
        //unorderedWait表示数据不需要关注顺序,处理完立即发送
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideDS,
                new AsyncDimFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimJSON) {
                        if (dimJSON != null) {
                            orderWide.setProvince_name(dimJSON.getString("NAME"));
                            orderWide.setProvince_area_code(dimJSON.getString("AREA_CODE"));
                            orderWide.setProvince_iso_code(dimJSON.getString("ISO_CODE"));
                            orderWide.setProvince_3166_2_code(dimJSON.getString("ISO_3166_2"));

                        }
                    }
                },
                100,
                TimeUnit.SECONDS
        );

        //5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimJSON) throws ParseException {

                        orderWide.setSku_name(dimJSON.getString("SKU_NAME"));
                        orderWide.setCategory3_name(dimJSON.getString("CATEGORY3_ID"));
                        orderWide.setSpu_id(dimJSON.getLong("Spu_id"));
                        orderWide.setTm_id(dimJSON.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS
        );
        //5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimJSON) throws ParseException {
                        orderWide.setSpu_name(dimJSON.getString("SPU_NAME"));

                    }
                }, 60, TimeUnit.SECONDS
        );
        //5.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideDS, new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARR") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimJSON) throws ParseException {
                        orderWide.setUser_gender(dimJSON.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS
        );

        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>>>>");

        //TODO 6 将数据写入Kafka
        orderWideWithCategory3DS.map(JSON::toJSONString)
                        .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        env.execute();
    }
}
