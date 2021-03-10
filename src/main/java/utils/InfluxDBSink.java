package utils;

import bean.PowerBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBSink extends RichSinkFunction<PowerBean> {
    private InfluxDB connect = null;
    private final String dataBaseName = "PMU_Power";
    private final String dbURL = "http://192.168.1.103:**************";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connect = InfluxDBFactory.connect(dbURL, "root", "root");
        connect.enableBatch(500, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(PowerBean value, Context context) throws Exception {
        Point.Builder builder = Point.measurement("PMUTest")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("PMU_ID", Integer.toString(value.PMU_ID))
                .addField("Latency", System.currentTimeMillis() - value.Timestamp)
                .addField("default", "default");

        Point p = builder.build();
        connect.write(dataBaseName, "autogen", p);
    }
}
