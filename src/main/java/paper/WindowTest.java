package paper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import scala.Int;

import static utils.FlinkUtils.getEnv;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStreamSource<String> socketTextStream = env.socketTextStream(args[0], Integer.parseInt(args[1]));
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = socketTextStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyed.timeWindow(Time.seconds(5));
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = windowedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                value1.f1 = value1.f1 + value2.f1;
                return value1;
            }
        });
        reduce.print();
        env.execute("windowWordCount");
    }
}
