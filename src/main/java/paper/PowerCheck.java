package paper;


import bean.PowerBean;
import bean.SourceBean;
import bean.WindowBean;
import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;
import utils.MyFunctions;

import java.util.Optional;
import java.util.Properties;

import static utils.FlinkUtils.*;
import static utils.MathUtils.sourceToPower;

public class PowerCheck {
    public static void main(String[] args) throws Exception {
//        String filePath = "src/main/resources/2019-03-23_00h_UTC_PMUID02.txt";

        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        Properties prop = completeProps(fromArgs);
        setProperties(prop);

        FlinkKafkaProducer.Semantic producerSemantic;
        if (Boolean.parseBoolean(getProperties().getProperty("enable.checkpoint"))) {
            enableKafkaExactlyOnce(Long.parseLong(getProperties().getProperty("checkpointInterval", "10000")));
            producerSemantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
        } else
            producerSemantic = FlinkKafkaProducer.Semantic.AT_LEAST_ONCE;


        DataStream<String> line = dataStreamFromKafka();

        SingleOutputStreamOperator<PowerBean> originalPowerBeans = line.map(new MapFunction<String, PowerBean>() {
            @Override
            public PowerBean map(String s) throws Exception {
                SourceBean sourceBean = JSON.parseObject(s, SourceBean.class);
                return sourceToPower(sourceBean);
            }
        });

        if (!Boolean.parseBoolean(getProperties().getProperty("onlyWindowFunction", "false"))) {
            FlinkKafkaProducer powerBeanProducer = getKafkaProducer(
                    PowerBean.class.getSimpleName(), producerSemantic, 5
            );
            originalPowerBeans.addSink(powerBeanProducer);
        }

        DataStream<PowerBean> inputStream = assignWatermark(originalPowerBeans);


        if (Boolean.parseBoolean(getProperties().getProperty("enable.windowFunction", "false"))) {
            WindowedStream<PowerBean, Integer, TimeWindow> windowedStream = inputStream
                    .keyBy(new KeySelector<PowerBean, Integer>() {
                        @Override
                        public Integer getKey(PowerBean bean) throws Exception {
                            return bean.PMU_ID;
                        }
                    }).window(SlidingEventTimeWindows.of(
                            Time.milliseconds(Long.parseLong(getProperties().getProperty("lengthOfWindow", "60000"))),
                            Time.milliseconds(Long.parseLong(getProperties().getProperty("lengthOfWindowSlide", "30000")))));

            SingleOutputStreamOperator<WindowBean> windowOut = windowedStream.aggregate(
                    new MyFunctions.WindowAggregate(), new MyFunctions.WindowProcess()
            );
            FlinkKafkaProducer windowBeanProducer = getKafkaProducer(
                    WindowBean.class.getSimpleName(), producerSemantic, 5
            );
            windowOut.addSink(windowBeanProducer);
        }

        executeJob();

    }

    private static Properties completeProps(ParameterTool fromArgs) {

        Properties prop = new Properties();

        prop.setProperty("source.topic", fromArgs.get("source.topic", "topic-A"));
        prop.setProperty("bootstrap.servers", fromArgs.get("bootstrap.servers", "192.168.1.103:31090,192.168.1.103:31091,192.168.1.103:31092"));
        prop.setProperty("group.id", fromArgs.get("group.id", "demo1"));
        prop.setProperty("auto.offset.reset", fromArgs.get("auto.offset.reset", "earliest"));
        prop.setProperty("enable.auto.submit", fromArgs.get("enable.auto.submit", "false"));
        prop.setProperty("jobName", fromArgs.get("jobName", "defaultJobName"));
        prop.setProperty("MaxParallelism", fromArgs.get("MaxParallelism", "4"));
        prop.setProperty("kafkaParallelism", fromArgs.get("kafkaParallelism", "3"));

        prop.setProperty("allowMaxTimeDelay", fromArgs.get("allowMaxTimeDelay", "1000"));
        // 划窗计算窗长ms
        prop.setProperty("lengthOfWindow", fromArgs.get("lengthOfWindow", "60000"));
        // 划窗计算，窗移动的时长ms
        prop.setProperty("lengthOfWindowSlide", fromArgs.get("lengthOfWindowSlide", "30000"));

        //每两次checkpoint之间的时间间隔 ms
        prop.setProperty("checkpointInterval", fromArgs.get("checkpointInterval", "10000"));
        prop.setProperty("transaction.timeout.ms", fromArgs.get("transaction.timeout.ms", "300000"));
        prop.setProperty("sink.powerBean.topic", fromArgs.get("sink.powerBean.topic", "topic-P"));
        prop.setProperty("sink.windowBean.topic", fromArgs.get("sink.windowBean.topic", "topic-W"));

        // 关闭Stream计算功能，只开启划窗计算
        prop.setProperty("onlyWindowFunction", fromArgs.get("onlyWindowFunction", "false"));
        if (Boolean.parseBoolean(prop.getProperty("onlyWindowFunction")))
            prop.setProperty("enable.windowFunction", "true");
        else
            // 添加划窗计算的功能，默认为不开启  通过命令行参数传入 -enable.windowFunction true  开启功能
            prop.setProperty("enable.windowFunction", fromArgs.get("enable.windowFunction", "false"));

        prop.setProperty("enable.checkpoint", fromArgs.get("enable.checkpoint", "false"));

        return prop;
    }


}
