package com.ls.app.Fucn;

import com.alibaba.fastjson.JSONObject;
import com.ls.utils.DimUtil;
import com.ls.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements AsyncJoinFunction<T> {

    //定义线程池
    private ThreadPoolExecutor poolExecutor;

    //定义关联的维度表的表名属性
    private String tableName;

    public AsyncDimFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        poolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {

        poolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {

                String key = getKey(t);

                //1.查询维度信息
                JSONObject dim = DimUtil.getDim(tableName, key);

                //2.补充维度信息
                join(t, dim);

                //3.返回数据
                resultFuture.complete(Collections.singleton(t));
            }
        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut>>>>>>>>>>>>" + input);
    }
}