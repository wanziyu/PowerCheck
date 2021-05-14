package utils;

import bean.PowerBean;
import bean.WindowBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class WindowInfluxDBSink extends RichSinkFunction<WindowBean> {
    private InfluxDB connect = null;
    private String dataBaseName;
    private String dbURL;

    public WindowInfluxDBSink(String dbURL, String dataBaseName) {
        this.dbURL = dbURL;
        this.dataBaseName = dataBaseName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connect = InfluxDBFactory.connect(dbURL, "root", "root");
        connect.enableBatch(500, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connect.close();
    }

    @Override
    public void invoke(WindowBean value, Context context) throws Exception {
        Point.Builder builder = Point.measurement("PMUWindow")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("PMUID", Integer.toString(value.PMU_ID))
                .addField("Zone", 1)
                .addField("ActAverage", (float) value.ActAverage)
                .addField("ReaAverage", (float) value.ReaAverage)
                .addField("AppAverage", (float) value.AppAverage)
                .addField("PFAverage", (float) value.PFAverage)
                .addField("WindowStart", value.windowStartTimestamp);

        Point p = builder.build();
        connect.write(dataBaseName, "autogen", p);
    }
}
