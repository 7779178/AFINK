package com.ls.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.ls.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {

    private static Connection connection;

    private static Connection getConnection() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            return DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败！");
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> clz, Boolean underScoreToCamel) {

        //1.获取连接
        if (connection == null) {
            connection = getConnection();
        }

        PreparedStatement preparedStatement = null;

        ResultSet resultSet = null;
        ArrayList<T> resultList = new ArrayList<>();

        try {
            //2.编译SQL
            preparedStatement = connection.prepareStatement(sql);

            //3.执行查询
            resultSet = preparedStatement.executeQuery();

            //4.处理resultSet结果集
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {

                //创建泛型对象
                T t = clz.newInstance();

                for (int i = 1; i < columnCount + 1; i++) {

                    //获取列名
                    String columnName = metaData.getColumnName(i);
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    }

                    //获取列值
                    String value = resultSet.getString(columnName);

                    //给泛型对象赋值
                    BeanUtils.setProperty(t, columnName, value);
                }

                //将泛型对象加入集合
                resultList.add(t);
            }


        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("查询维度" + sql + "信息失败");
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return resultList;

    }

    public static void main(String[] args) {

        List<JSONObject> queryList = queryList("select * from GMALL2021_REALTIME.DIM_USER_INFO where id ='17'", JSONObject.class, false);

        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }

    }

}