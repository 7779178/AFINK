package com.ls.utils;

import com.ls.bean.TransientSink;
import com.ls.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction <T> getClickHouseSink(String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //通过反射的方式获取所有的列名,包含私有的属性
                        Field[] fields = t.getClass().getDeclaredFields();

                        //遍历fields,通过反射的方式获取属性值并赋值给预编译的SQL
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            //获取属性名
                            Field field = fields[i];

                            //获取该字段上的注解信息
                            TransientSink annotation = field.getAnnotation(TransientSink.class);

                            if (annotation != null) {
                                offset++;
                                continue;
                            }

                            //通过反射的方式获取属性值
                            field.setAccessible(true);

                            try {
                                preparedStatement.setObject(i + 1 - offset, field.get(t));
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICK_HOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

}
