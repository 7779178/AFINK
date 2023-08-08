package com.ls.app.Fucn;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface AsyncJoinFunction<T> {

    String getKey(T t);

    void join(T t, JSONObject dimJSON) throws ParseException;

}
