package utils;

import bean.PmuIdBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Collector;
import bean.PowerBean;
import bean.WindowBean;

import java.nio.charset.StandardCharsets;

public class MyFunctions {

    public static class WindowAggregate implements AggregateFunction<PowerBean,
            Tuple4<Desc, Desc, Desc, Desc>, WindowBean> {

        @Override
        public Tuple4<Desc, Desc, Desc, Desc> createAccumulator() {
            return Tuple4.of(new Desc(), new Desc(), new Desc(), new Desc());
        }

        @Override
        public Tuple4<Desc, Desc, Desc, Desc> add(PowerBean bean, Tuple4<Desc, Desc, Desc, Desc> desc4) {
            Desc act = desc4.f0;
            Desc rea = desc4.f1;
            Desc app = desc4.f2;
            Desc pf = desc4.f3;
            act.update(bean.ActPower_Sum);
            rea.update(bean.ReaPower_Sum);
            app.update(bean.AppPower_Sum);
            pf.update(bean.PF);
            return desc4;
        }

        @Override
        public WindowBean getResult(Tuple4<Desc, Desc, Desc, Desc> desc4) {
            //pmu_id 此时 先暂存为-1, process时赋值
            return WindowBean.of(-1, 0L,
                    desc4.f0.average(), desc4.f1.average(), desc4.f2.average(), desc4.f3.average(),
                    desc4.f0.max, desc4.f1.max, desc4.f2.max, desc4.f3.max,
                    desc4.f0.min, desc4.f1.min, desc4.f2.min, desc4.f3.min);
        }

        @Override
        public Tuple4<Desc, Desc, Desc, Desc> merge(Tuple4<Desc, Desc, Desc, Desc> d1, Tuple4<Desc, Desc, Desc, Desc> d2) {
            d1.f0.merge(d2.f0);
            d1.f1.merge(d2.f1);
            d1.f2.merge(d2.f2);
            d1.f3.merge(d2.f3);
            return d1;
        }
    }

    public static class WindowProcess extends ProcessWindowFunction<WindowBean, WindowBean, Integer, TimeWindow> {
        @Override
        public void process(Integer pmu_id, Context context, Iterable<WindowBean> elements, Collector<WindowBean> out) throws Exception {
            WindowBean windowBean = elements.iterator().next();
            windowBean.pmuId = pmu_id;
            windowBean.windowStartTimestamp = context.window().getStart();
            out.collect(windowBean);
        }
    }

    public static class Desc {
        public double sum;
        public double min;
        public double max;
        public long count;

        public Desc() {
            this.count = 0L;
            this.sum = 0;
            this.min = Double.MIN_VALUE;
            this.max = Double.MAX_VALUE;
        }

        public void update(double value) {
            if (count == 0)
                this.sum = min = max = value;
            else {
                this.sum += value;
                min = min < value ? min : value;
                max = max > value ? max : value;
            }
            count++;
        }

        public double average() {
            return sum / count;
        }

        public void merge(Desc desc) {
            this.sum += desc.sum;
            this.count += desc.count;
            this.min = this.min < desc.min ? this.min : desc.min;
            this.max = this.max > desc.max ? this.max : desc.max;
        }
    }

    public static class KafkaPmuIdPartitioner<T extends PmuIdBean> extends FlinkKafkaPartitioner<PmuIdBean> {

        @Override
        public int partition(PmuIdBean record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return record.getPmuId() % partitions.length;
        }
    }

    public static class MySerializationSchema<T> implements SerializationSchema<Object>{
        @Override
        public byte[] serialize(Object element) {
            return JSON.toJSONString(element).getBytes(StandardCharsets.UTF_8);
        }
    }
}
