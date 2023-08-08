package com.ls.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// TODO: 2023/7/23 加上注解，加载时不进行序列化
// TODO: 2023/7/23 第一个注解：应用于字段上
// TODO: 2023/7/23 第二个主节：运行时生效
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
