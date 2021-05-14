package utils;

import bean.PowerBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class StreamInfluxDBSink extends RichSinkFunction<PowerBean> {
    private InfluxDB connect = null;
    private String dataBaseName;
    private String dbURL;

    public StreamInfluxDBSink(String dbURL, String dataBaseName) {
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
    public void invoke(PowerBean value, Context context) throws Exception {
        Point.Builder builder = Point.measurement("PMUStream")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("PMUID", Integer.toString(value.PMU_ID))
                .addField("Latency", System.currentTimeMillis() - value.Timestamp)
                .addField("A_Active", (float) value.ActPower_A)
                .addField("B_Active", (float) value.ActPower_B)
                .addField("C_Active", (int) value.ActPower_C)
                .addField("Sum_Active", (int) value.ActPower_Sum)
                .addField("A_Reactive", (int) value.ReaPower_A)
                .addField("B_Reactive", (int) value.ReaPower_B)
                .addField("C_Reactive", (int) value.ReaPower_C)
                .addField("Sum_Reactive", (int) value.ReaPower_Sum)
                .addField("A_Apparent", (int) value.AppPower_A)
                .addField("B_Apparent", (int) value.AppPower_B)
                .addField("C_Apparent", (int) value.AppPower_C)
                .addField("Sum_Apparent", (int) value.AppPower_Sum)
                .addField("PF", (float) value.PF);

        Point p = builder.build();
        connect.write(dataBaseName, "autogen", p);
    }
}
