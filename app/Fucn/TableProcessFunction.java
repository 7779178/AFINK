package com.ls.app.Fucn;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ls.bean.TableProcess;
import com.ls.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //定义Phoenix连接
    private Connection connection;

    //定义侧输出流标记
    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value :{"database":"gmall2021-realtime","data":{"operate_type":"insert","sink_type":"kafka","sink_table":"dim_base_trademark","source_table":"base_trademark","sink_pk":"id","sink_columns":"id,tm_name"},"type":"insert","table":"table_process"}
    @Override
    public void processBroadcastElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

        //将单条数据转换为JSON对象
        JSONObject jsonObject = JSON.parseObject(value);

        //获取其中的data
        String data = jsonObject.getString("data");

        //将data转换为TableProcess对象
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //取出输出类型
        String sinkType = tableProcess.getSinkType();
        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //获取状态
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);

        //准备状态中的Key
        String sourceTable = tableProcess.getSourceTable();
        String operateType = tableProcess.getOperateType();
        String key = sourceTable + ":" + operateType;

        //将数据写入状态进行广播
        broadcastState.put(key, tableProcess);

    }
    //核心处理方法，根据MySQL配置表的信息为每条数据打标签，走kafka还是hbase
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //取出状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        //拼接Key
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        String key = table + ":" + type;

        //获取单条数据对应的配置信息
        TableProcess tableProcess = broadcastState.get(key);

        //判断配置信息是否存在
        if (tableProcess != null) {

            //过滤字段
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());

            //分流,Kafka数据写入主流,Phoenix数据写入侧输出流
            String sinkType = tableProcess.getSinkType();

            //获取Kafka主题或者Phoenix表名
            String sinkTable = tableProcess.getSinkTable();
            jsonObject.put("sinkTable", sinkTable);

            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                readOnlyContext.output(outputTag, jsonObject);
            }

        } else {
           System.out.println("配置信息中不存在Key:" + key);
        }

    }

    //建表SQL: create table t(id varchar primary key,name varchar,sex varchar) ...;
    //如果MySQL的配置表中添加了数据，该方法用于检查HBase中是否创建过表，如果没有则通过Phoenix中创建新增的表
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        //处理主键以及扩展字段
        if (sinkPk == null || sinkPk.equals("")) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        //准备SQL语句
        StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            createTableSQL.append(column).append(" varchar ");

            //如果当前字段为主键,则添加主键属性
            if (sinkPk.equals(column)) {
                createTableSQL.append(" primary key ");
            }

            //如果当前字段不是最后一个字段,则添加","
            if (i < columns.length - 1) {
                createTableSQL.append(",");
            }
        }

        //补充右边括号以及扩展语句
        createTableSQL.append(")")
                .append(sinkExtend);

        System.out.println(createTableSQL.toString());
        PreparedStatement preparedStatement = null;
        try {
            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Phoenix建表" + sinkTable + "失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    //根据指定的字段对数据进行过滤,校验字段，过滤掉多余的字段
    private void filterColumn(JSONObject data, String sinkColumns) {

        //获取data数据中的数据集
        Set<Map.Entry<String, Object>> entries = data.entrySet();

        //对需要保留的字段做切分并转换为List
        String[] columnsArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnsArr);



//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        //遍历数据集,如果在需要保留的字段中不存在,则移除该字段
        entries.removeIf(next -> !columnList.contains(next.getKey()));

    }

}
