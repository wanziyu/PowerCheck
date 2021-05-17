package paper;

import bean.PowerBean;
import bean.SourceBean;
import bean.WindowBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Int;
import scala.collection.immutable.Stream;
import utils.MyFunctions;

import java.util.Properties;

import static utils.FlinkUtils.*;
import static utils.FlinkUtils.executeJob;
import static utils.MathUtils.sourceToPower;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);
        DataStreamSource<String> line = env.readTextFile("D:\\Project\\PowerCheck\\src\\main\\resources\\2019-03-23_00h_UTC_PMUID02.txt");

//        line.print();

        SingleOutputStreamOperator<PowerBean> originalPowerBeans = line.map(new MapFunction<String, PowerBean>() {
            @Override
            public PowerBean map(String s) throws Exception {
                SourceBean sourceBean = JSON.parseObject(s, SourceBean.class);
                return sourceToPower(sourceBean);
            }
        });


        SingleOutputStreamOperator<PowerBean> marked = originalPowerBeans.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<PowerBean>(Time.milliseconds(0L)) {
                    @Override
                    public long extractTimestamp(PowerBean element) {
                        return element.Timestamp;
                    }
                }
        ).setParallelism(1);

//        marked.print();
        KeyedStream<PowerBean, Integer> keyed = marked
                .keyBy(new KeySelector<PowerBean, Integer>() {
                    @Override
                    public Integer getKey(PowerBean bean) throws Exception {
                        return bean.PMU_ID;
                    }
                });


        WindowedStream<PowerBean, Integer, TimeWindow> windowedStream = keyed.window(SlidingEventTimeWindows.of(
                Time.milliseconds(60000L),
                Time.milliseconds(30000L)));

        SingleOutputStreamOperator<WindowBean> windowOut = windowedStream.aggregate(
                new MyFunctions.WindowAggregate(), new MyFunctions.WindowProcess()
        );
//        SingleOutputStreamOperator<String> out = windowOut.map(new MapFunction<WindowBean, String>() {
//            @Override
//            public String map(WindowBean value) throws Exception {
//                return JSON.toJSONString(value);
//            }
//        });

        windowOut.writeAsText("window");

//        out.print();
        env.execute("windowWordCount");

    }
}
