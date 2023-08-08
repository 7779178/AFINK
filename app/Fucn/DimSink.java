package com.ls.app.Fucn;

import com.alibaba.fastjson.JSONObject;
import com.ls.common.GmallConfig;
import com.ls.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {

    //声明Phoenix连接
    private Connection connection;

    //生命周期方法
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * @param jsonObject {"database":"","table":"user_info","type":"","data":{"id":"11"...},"sinkTable":"dim_user_info"}
     * @param context
     * @throws Exception
     */
    @Override  //SQL : upsert into schema.table (id,name,sex) values(..,..,..)
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

        PreparedStatement preparedStatement = null;

        //获取表名
        String table = jsonObject.getString("sinkTable");

        //获取列名和列值
        JSONObject data = jsonObject.getJSONObject("data");
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        try {
            //拼接SQL语句
            String upsertSql = genUpertSql(table, columns, values);

            //预编译SQL
            System.out.println(upsertSql);
            preparedStatement = connection.prepareStatement(upsertSql);

            //执行
            preparedStatement.execute();

            //提交数据
            connection.commit();

            //判断如果为更新数据,则删除Redis中数据
            if ("update".equals(jsonObject.getString("type"))) {
                String value = jsonObject.getJSONObject("data").getString("id");
                String key = table + ":" + value;
                DimUtil.deleteCache(key);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入维度表：" + table + "数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    //SQL : upsert into schema.table (id,name,sex) values('..','..','..')
    private String genUpertSql(String table, Set<String> columns, Collection<Object> values) {
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + table +
                "(" + StringUtils.join(columns, ",") + ")" +
                " values('" + StringUtils.join(values, "','") + "')";
    }
}
