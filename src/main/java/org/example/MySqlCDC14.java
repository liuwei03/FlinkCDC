//package org.example;
//
//import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
//import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
///**
// * Flink CDC 1.4.0 版本支持断点续传，新版本暂未支持
// */
//public class MySqlCDC14 {
//
//    public static void main(String[] args)  throws Exception{
//
//        // 连接数据库
//        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                .hostname("hadoop1")
//                .port(3306)
//                .databaseList("test")
//                .username("root")
//                .password("Founder123")
//                .deserializer(new StringDebeziumDeserializationSchema())
//                .build();
//
//        // 创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 获取输入流
//        DataStreamSource<String> source = env.addSource(sourceFunction);
//
//        // 输出到控制台
//        source.print().setParallelism(1);
//
//        // 执行
//        env.execute();
//
//    }
//
//}
