package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MySqlCDC22 {

    public static void main(String[] args)  throws Exception{

        Properties properties = new Properties();
        properties.setProperty("useSSL","false");

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");

        // 监听Mysql
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop1")
                .port(3306)
                .databaseList("test")
                .tableList("test.*")
                .username("root")
                .password("Founder123")
                .deserializer(new JsonDebeziumDeserializationSchema(true,customConverterConfigs))
                .jdbcProperties(properties)
                .build();

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建检查点
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/lw/project/bdyx_project/FlinkCDC/src/main/resources");

        // 执行 打印到控制台
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
           .setParallelism(4)
           .print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }

}
