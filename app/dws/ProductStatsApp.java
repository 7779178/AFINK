package com.ls.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ls.app.Fucn.AsyncDimFunction;
import com.ls.bean.OrderWide;
import com.ls.bean.PaymentWide;
import com.ls.bean.ProductStats;
import com.ls.common.GmallConstant;
import com.ls.utils.ClickHouseUtil;
import com.ls.utils.DateTimeUtil;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 读取7个主题的数据创建流
        String groupId ="product_stats_app_0923_group";

        //2.1 页面数据流 点击和曝光
        String pageTopic ="dwd_page_log";
        FlinkKafkaConsumer<String> pageSource = MyKafkaUtil.getKafkaSource(groupId, pageTopic);
        DataStreamSource<String> pageLogDS = env.addSource(pageSource);

        //2.2 收藏数据流 收藏
        String favoTapic="dwd_favor_info";
        FlinkKafkaConsumer<String> favoSource = MyKafkaUtil.getKafkaSource(groupId, favoTapic);
        DataStreamSource<String> favoDS = env.addSource(favoSource);

        //2.3 加购数据流
        String cartTopic ="dwd_cart_info";
        FlinkKafkaConsumer<String> cartSource = MyKafkaUtil.getKafkaSource(groupId, cartTopic);
        DataStreamSource<String> cartDS = env.addSource(cartSource);

        //2.4 订单数据流
        String orderTopic = "dwm_order_wide";
        FlinkKafkaConsumer<String> orderSource = MyKafkaUtil.getKafkaSource(groupId, orderTopic);
        DataStreamSource<String> orderDS = env.addSource(orderSource);

        //2.5 支付数据流
        String paymentTopic = "dwm_payment_wide";
        FlinkKafkaConsumer<String> paymentSource = MyKafkaUtil.getKafkaSource(groupId, paymentTopic);
        DataStreamSource<String> paymentDS = env.addSource(paymentSource);

        //2.6 退款数据流
        String refundTopic = "dwd_order_refund_info";
        FlinkKafkaConsumer<String> refundSource = MyKafkaUtil.getKafkaSource(groupId, refundTopic);
        DataStreamSource<String> refundDS = env.addSource(refundSource);

        //2.7 评价数据流
        String commentTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> commentSource = MyKafkaUtil.getKafkaSource(groupId, commentTopic);
        DataStreamSource<String> commentDS = env.addSource(commentSource);

        //TODO 3 统一数据格式

        //3.1 点击和曝光数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPageDS = pageLogDS.flatMap(new FlatMapFunction<String, ProductStats>() {

            @Override
            public void flatMap(String s, Collector<ProductStats> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                //获取页面id
                JSONObject page = jsonObject.getJSONObject("page");
                String page_id = page.getString("page_id");
                String item_type = page.getString("item_type");
                if ("good_detsil".equals(page_id) && "sku_id".equals(item_type)) {
                    Long item = page.getLong("item");
                    collector.collect(ProductStats.builder().
                            sku_id(item)
                            .click_ct(1L)
                            .ts(jsonObject.getLong("ts"))
                            .build()
                    );
                }

            //获取曝光字段
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size()>0 ){
                    for(int i =0; i<displays.size();i++){
                        JSONObject display = displays.getJSONObject(i);

                        //取出曝光数据类型
                        String displayType = display.getString("item_type");
                        if ("sku_id".equals(displayType)){
                            Long skuId = display.getLong("item");

                            collector.collect(ProductStats.builder()
                                    .sku_id(skuId)
                                    .display_ct(1L)
                                    .ts(jsonObject.getLong("ts"))
                                    .build()
                            );
                        }
                    }
                }
            }
        });

        //3.2 收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavoDS = favoDS.map(data -> {
            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(data);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //取出sku_id
            Long sku_id = jsonObject.getLong("sku_id");
            //写出数据
            return ProductStats.builder()
                    .sku_id(sku_id)
                    .favor_ct(1L)
                    .ts(sdf.parse(jsonObject.getString("create_time")).getTime())
                    .build();
        });

        //3.3 加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(
                json -> {
                    JSONObject cartInfo = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(cartInfo.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(cartInfo.getLong("sku_id"))
                            .cart_ct(1L)
                            .ts(ts)
                            .build();
                });

        //3.4 订单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(data -> {

            OrderWide orderWide = JSON.parseObject(data, OrderWide.class);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .orderIdSet(orderIds)                  //为了最后计算订单总数
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getTotal_amount())
                    .ts(sdf.parse(orderWide.getCreate_time()).getTime())
                    .build();
        });

        //3.5 支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentDS.map(
                json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                    return ProductStats.builder()
                            .sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts)
                            .build();
                });

        //3.6 退单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(
                json -> {
                    JSONObject refundJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts)
                            .build();
                });


        //3.7 评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(data -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(data);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            //取出sku_id和评价类型
            Long sku_id = jsonObject.getLong("sku_id");
            String appraise = jsonObject.getString("appraise");

            Long goodAppraise = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodAppraise = 1L;
            }

            return ProductStats.builder()
                    .sku_id(sku_id)
                    .comment_ct(1L)
                    .good_comment_ct(goodAppraise)
                    .ts(sdf.parse(jsonObject.getString("create_time")).getTime())
                    .build();
        });

        //TODO 4 Union7个流
        DataStream<ProductStats> unionDS = productStatsWithPageDS.union(productStatsWithFavoDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);

        //TODO 5 提取时间戳生成WaterMark
        SingleOutputStreamOperator<ProductStats> productStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                            @Override
                            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        //TODO 6 分组、开窗、聚合
        KeyedStream<ProductStats, Long> keyedStream = productStatsWithWmDS.keyBy(ProductStats::getSku_id);
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(new ReduceFunction<ProductStats>() {
            @Override
            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());

                stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                return stats1;
            }
        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
            @Override
            public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {

                //取出窗口开始和结束时间
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();

                String stt = DateTimeUtil.toYMDhms(new Date(start));
                String edt = DateTimeUtil.toYMDhms(new Date(end));

                //将窗口时间设置进JavaBean对象中
                ProductStats productStats = iterable.iterator().next();
                productStats.setStt(stt);
                productStats.setEdt(edt);

                //写入数据
                collector.collect(productStats);
            }
        });

        //TODO 7 关联维度信息,补充维度数据

        //7.1 关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new AsyncDimFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        String skuId = productStats.getSku_id().toString();
                        System.out.println("SkuId::::::::::::::" + skuId);
                        return skuId;
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimJSON) {

                        //取出商品信息表中的价格和名称
                        String sku_name = dimJSON.getString("SKU_NAME");
                        BigDecimal price = dimJSON.getBigDecimal("PRICE");

                        Long spu_id = dimJSON.getLong("SPU_ID");
                        Long tm_id = dimJSON.getLong("TM_ID");
                        Long category3_id = dimJSON.getLong("CATEGORY3_ID");

                        System.out.println("Spu:" + spu_id + ",TmId:" + tm_id + ",category3_id:" + category3_id + ",Price" + price);

                        //将维度信息补充完成
                        productStats.setSku_name(sku_name);
                        productStats.setSku_price(price);
                        productStats.setSpu_id(spu_id);
                        productStats.setTm_id(tm_id);
                        productStats.setCategory3_id(category3_id);
                    }
                },
                100,
                TimeUnit.SECONDS);

//        productStatsWithSkuDS.print(">>>>>>>>>>>>>>>>>>>>>>>>");

        //7.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new AsyncDimFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                Long spu_id = productStats.getSpu_id();
                                System.out.println("Spu_id:::::::::::::" + spu_id);
                                return String.valueOf(spu_id);
                            }
                        }, 60, TimeUnit.SECONDS);

//        productStatsWithSpuDS.print(">>>>>>>>>>>>>>>>>>>>");

        //7.3 关联TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new AsyncDimFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                Long tm_id = productStats.getTm_id();
                                System.out.println("tm_id::::::::::::::::" + tm_id);
                                return String.valueOf(tm_id);
                            }
                        }, 60, TimeUnit.SECONDS);
//        productStatsWithTmDS.print(">>>>>>>>>>>>>>>>>>>>>");

        //7.4 关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithTmDS,
                        new AsyncDimFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                Long category3_id = productStats.getCategory3_id();
                                System.out.println("category3_id::::::::::::::" + category3_id);
                                return String.valueOf(category3_id);
                            }
                        }, 60, TimeUnit.SECONDS);

        //TODO 8 将数据写入ClickHouse
        productStatsWithCategory3DS.print(">>>>>>>>>>>>>>>>>>");
        productStatsWithCategory3DS.addSink(ClickHouseUtil.getClickHouseSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9 启动任务
        env.execute();

    }
}
