package com.ls.app.dws;

import com.ibm.icu.text.PluralRules;
import com.ls.app.Fucn.KeywordUDTF;
import com.ls.bean.KeywordStats;
import com.ls.common.GmallConstant;
import com.ls.utils.ClickHouseUtil;
import com.ls.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useAnyPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2 读取Kafka dwd_page_log 主题数据
        String topic ="dwd_page_log";
        String groupId="keyword_stats_app_0923";

        tableEnv.executeSql("create table page_log("+
                "   common Map<String,String>,"+
                "   page MAP<String,String>,"+
                "   ts BIGINT,"+
                "   rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(TS/1000)),"+
                "   WATERMARK FOR rowtime AS rowtime"+
                "   ) with (" + MyKafkaUtil.getKafkaDDL(topic,groupId) + ")");

        //TODO 3 过滤数据集
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "   page['item'] fullWords, " +
                "   rowtime " +
                "from page_log " +
                "where page['last_page_id']='search' and page['item'] is not null");
        tableEnv.createTemporaryView("filterTable",filterTable);

        //TODO 4 注册函数并使用UDTF函数对搜索关键词进行切分
        tableEnv.createTemporarySystemFunction("split_words", KeywordUDTF.class);
        Table splitTable = tableEnv.sqlQuery("select" +
                "   word, " +
                "   rowtime " +
                "from filterTable, LATERAL TABLE(split_words(fullWords))");

        //TODO 5 分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("select '" +
                "   date_format(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss')sst," +
                "   date_formt(TUMBLE_END(rowtime, INTERVAL '10' SECOND)),'yyyy-MM-dd HH:mm:ss')edt," +
                "   word keyword," +
                "   count(*) ct, " +
                "   UNIX_TIMESTAMP()*1000 ts " +
                "from splitTable " +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND)," +
                "   word");

        //TODO 6 将数据转换为流写入ClickHouse
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);
        keywordStatsDataStream.print();
        keywordStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));


        //TODO 7 启动任务
        env.execute();
    }
}
