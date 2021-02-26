package bean;

import com.alibaba.fastjson.JSON;

public class WindowBean implements PmuIdBean {

    public int PMU_ID;
    public long windowStartTimestamp;
    public double ActAverage;
    public double ReaAverage;
    public double AppAverage;

    public double ActHigh;
    public double ReaHigh;
    public double AppHigh;

    public double ActLow;
    public double ReaLow;
    public double AppLow;

    public WindowBean() {
    }

    public WindowBean(int PMU_ID, long windowStartTimestamp, double actAverage, double reaAverage,
                      double appAverage, double actHigh, double reaHigh, double appHigh,
                      double actLow, double reaLow, double appLow) {
        this.PMU_ID = PMU_ID;
        this.windowStartTimestamp = windowStartTimestamp;
        ActAverage = actAverage;
        ReaAverage = reaAverage;
        AppAverage = appAverage;
        ActHigh = actHigh;
        ReaHigh = reaHigh;
        AppHigh = appHigh;
        ActLow = actLow;
        ReaLow = reaLow;
        AppLow = appLow;
    }

    public static WindowBean of(int PMU_ID, long windowStartTimestamp, double actAverage, double reaAverage,
                                double appAverage, double actHigh, double reaHigh, double appHigh,
                                double actLow, double reaLow, double appLow) {
        return new WindowBean(PMU_ID, windowStartTimestamp, actAverage, reaAverage,
                appAverage, actHigh, reaHigh, appHigh,
                actLow, reaLow, appLow);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    @Override
    public int getPmuId() {
        return this.PMU_ID;
    }
}
