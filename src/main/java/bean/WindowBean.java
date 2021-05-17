package bean;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WindowBean implements PmuIdBean {


    public int pmuId;
    public long windowStartTimestamp;

    public double actAverage;
    public double reaAverage;
    public double appAverage;
    public double pfAverage;

    public double actHigh;
    public double reaHigh;
    public double appHigh;
    public double pfHigh;

    public double actLow;
    public double reaLow;
    public double appLow;
    public double pfLow;



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
        return this.pmuId;
    }

    public static void main(String[] args) {
        WindowBean windowBean = new WindowBean();
        System.out.println(windowBean.toString());
    }
}
