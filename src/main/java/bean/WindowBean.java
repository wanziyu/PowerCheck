package bean;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WindowBean implements PmuIdBean {

    public int PMU_ID;
    public long windowStartTimestamp;
    public double ActAverage;
    public double ReaAverage;
    public double AppAverage;
    public double PFAverage;

    public double ActHigh;
    public double ReaHigh;
    public double AppHigh;
    public double PFHigh;

    public double ActLow;
    public double ReaLow;
    public double AppLow;
    public double PFLow;



    public static WindowBean of(int PMU_ID, long windowStartTimestamp,
                                double actAverage, double reaAverage, double appAverage, double PFAverage,
                                double actHigh, double reaHigh, double appHigh, double PFHigh,
                                double actLow, double reaLow, double appLow, double PFLow) {
        return new WindowBean(PMU_ID, windowStartTimestamp,
                actAverage, reaAverage, appAverage, PFAverage,
                actHigh, reaHigh, appHigh, PFHigh,
                actLow, reaLow, appLow, PFLow);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    @Override
    public int getPmuId() {
        return this.PMU_ID;
    }

    public static void main(String[] args) {
        WindowBean windowBean = new WindowBean();
        System.out.println(windowBean.toString());
    }
}
