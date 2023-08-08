package com.ls.common;

public class GmallConfig {

    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181";

    //ClickHouse驱动
    public static final String CLICK_HOUSE_DRIVER  ="ru.yandex.clickhouse.ClickHouseDriver";

    //ClickHouseUrl
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop101:8123/default";
}
