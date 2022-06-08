package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(Binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Phoenix(DIM)
//程  序：    Mock -> Mysql(Binlog) -> maxwell.sh -> Kafka(ZK) -> DimApp -> Phoenix(HBase HDFS/ZK)
public class DimApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生成环境设置为Kafka主题的分区数

//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");

        //TODO 2.读取 Kafka topic_db 主题数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("topic_db", "dim_app_211027"));

        //TODO 3.过滤掉非JSON格式的数据,并将其写入侧输出流

        // 2022-06-08 12:25:49
        //  split 将一个流分成多个流
        // select 获取分流之后对应的数据
        //  注意：split函数已经过期，并且移除
        //  side outputs：可以使用process方法对流中数据进行处理，并对不同的处理结果将数据收集到不同的outputtag中
        OutputTag<String> dirtyDataTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    // 2022-06-08 11:33:37
                    // 通过解析json异常捕获，异常数据，输入到测输出流
                    context.output(dirtyDataTag, s);
                }
            }
        });

        //取出脏数据并打印
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyDataTag);
        sideOutput.print("Dirty>>>>>>>>>>");

        //TODO 4.使用FlinkCDC读取MySQL中的配置信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-211027-config")
                .tableList("gmall-211027-config.table_process")
                // 2022-06-08 14:56:54
                //   DebeziumDeserializationSchema的JSON格式实现，它将接收到的SourceRecord反序列化为JSON字符串。
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 2022-06-08 14:49:43
                //  启动选项 initial，earliest，latest，specificOffset，timestamp
                //  initial 在第一次启动时对受监视的数据库表执行初始快照，并继续读取最新的binlog
                //  earliest    不要在第一次启动时对受监视的数据库表执行快照，只需从binlog的开头读取即可。因为只有当binlog保证包含数据库的整个历史时，它才有效。
                //  latest  不要在第一次启动时对受监控的数据库表执行快照，只要从binlog的末尾读取即可，这意味着只有在连接器启动后才有更改。
                //  specificOffset  在第一次启动时，决不对受监视的数据库表执行快照，而是直接从指定的偏移量读取binlog。
                //  timestamp   在第一次启动时，绝不对受监控的数据库表执行快照，并直接从指定的时间戳读取binlog。以毫秒为单位
                .startupOptions(StartupOptions.initial())
                .build();
        // 2022-06-08 15:06:20
        // WatermarkStrategy    接口
        //  常见的watermark策略
        //  noWatermarks    创建完全不生成水印的水印策略。这在执行纯处理时间流处理的场景中可能很有用。
        //  forGenerator    基于现有水印生成器供应商创建水印策略。
        //  forBoundedOutOfOrderness    为记录无序的情况创建水印策略，但可以设置事件无序程度的上限。无序边界B意味着一旦遇到时间戳为T的A事件，就不会再出现早于T-B的事件。 水印会定期生成。该水印策略引入的延迟是周期间隔长度加上无序边界。
        //  forMonotonousTimestamps 为时间戳单调递增的情况创建水印策略。水印会定期生成，并紧跟数据中最新的时间戳。该策略引入的延迟主要是水印生成的周期间隔。
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        //TODO 5.将配置信息流处理成广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);

        //TODO 6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //TODO 7.根据广播流数据处理主流数据
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));

        //TODO 8.将数据写出到Phoenix中
        hbaseDS.print(">>>>>>>>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());

        //TODO 9.启动任务
        env.execute("DimApp");

    }

}
