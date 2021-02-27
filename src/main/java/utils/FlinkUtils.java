package utils;

import bean.WindowBean;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import bean.PowerBean;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Optional;
import java.util.Properties;


public class FlinkUtils {
    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static Properties properties;


    public static void setMaxParallelism() {
        int maxParallelism = Integer.parseInt(properties.getProperty("MaxParallelism", "0"));
        env.getConfig().setMaxParallelism(maxParallelism);
    }


    public static DataStream<String> createFromTxt(String filePath) {
        return env.readTextFile(filePath);
    }

    public static DataStream<String> dataStreamFromKafka() throws Exception {
        String topic = properties.getProperty("source.topic", (String) null);
        if (topic == null) {
            throw new RuntimeException("source.topic is not defined in propertiesFile");
        }
        CheckpointingMode checkpointingMode = env.getCheckpointConfig().getCheckpointingMode();
        if (checkpointingMode == CheckpointingMode.EXACTLY_ONCE) {
            if (properties.getProperty("enable.auto.submit") != "false")
                properties.setProperty("enable.auto.submit", "false");
        }
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> dataStreamSource = env.addSource(consumer);
        return dataStreamSource;
    }

    public static void enableKafkaExactlyOnce(long interval) {
        env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE);
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }

    public static Properties getProperties() {
        return properties;
    }

    public static void setProperties(Properties properties) {
        FlinkUtils.properties = properties;
    }

    public static void executeJob() throws Exception {
        if (properties.containsKey("kafkaParallelism"))
            env.setParallelism(Integer.parseInt(properties.getProperty("kafkaParallelism")));
        if (properties.containsKey("jobName"))
            env.execute(properties.getProperty("jobName"));
        else
            env.execute();
    }

    public static DataStream<PowerBean> assignWatermark(DataStream<PowerBean> dataStream) {
        long allowMaxTimeDelay = Long.parseLong(properties.getProperty("allowMaxTimeDelay", "0"));
        SingleOutputStreamOperator<PowerBean> watermarksStream = dataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<PowerBean>(Time.milliseconds(allowMaxTimeDelay)) {
                    @Override
                    public long extractTimestamp(PowerBean element) {
                        return element.Timestamp;
                    }
                }
        );
        return watermarksStream;
    }

    public static FlinkKafkaProducer getKafkaProducer(String topicBean, FlinkKafkaProducer.Semantic semantic, int kafkaProducersPoolSize) {
        FlinkKafkaProducer producer;
        if ("powerbean".equals(topicBean.toLowerCase()))
             producer = new FlinkKafkaProducer(
                    properties.getProperty("sink.powerBean.topic"),
                    new MyFunctions.MySerializationSchema<PowerBean>(),
                    properties,
                    new MyFunctions.KafkaPmuIdPartitioner<PowerBean>(),
                    semantic,
                    kafkaProducersPoolSize
            );
        else if ("windowbean".equals(topicBean.toLowerCase()))
            producer = new FlinkKafkaProducer(
                    properties.getProperty("sink.windowBean.topic"),
                    new MyFunctions.MySerializationSchema<WindowBean>(),
                    properties,
                    new MyFunctions.KafkaPmuIdPartitioner<WindowBean>(),
                    semantic,
                    kafkaProducersPoolSize
            );
        else
            throw new RuntimeException(topicBean + "is not founded");
        return producer;
    }

}
