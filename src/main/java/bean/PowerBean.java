package bean;


import com.alibaba.fastjson.JSON;

public class PowerBean implements PmuIdBean {
    public int PMU_ID;
    public long Timestamp;
    public long FlinkProcessTime;
    public long FlinkDelayTime;
    public double ActPower_A; //有功功率
    public double ReaPower_A; //无功功率
    public double AppPower_A; //视在功率
    public double ActPower_B;
    public double ReaPower_B;
    public double AppPower_B;
    public double ActPower_C;
    public double ReaPower_C;
    public double AppPower_C;
    public double ActPower_Sum;
    public double ReaPower_Sum;
    public double AppPower_Sum;
    public double PF;
    public double UnbalancedRate;

    public PowerBean(int PMU_ID, long timestamp, long FlinkProcessTime, long FlinkDelayTime, double actPower_A,
                     double reaPower_A, double appPower_A, double actPower_B, double reaPower_B, double appPower_B,
                     double actPower_C, double reaPower_C, double appPower_C, double actPower_Sum, double reaPower_Sum,
                     double appPower_Sum, double PF, double UnbalancedRate) {
        this.PMU_ID = PMU_ID;
        Timestamp = timestamp;
        this.FlinkProcessTime = FlinkProcessTime;
        this.FlinkDelayTime = FlinkDelayTime;
        ActPower_A = actPower_A;
        ReaPower_A = reaPower_A;
        AppPower_A = appPower_A;
        ActPower_B = actPower_B;
        ReaPower_B = reaPower_B;
        AppPower_B = appPower_B;
        ActPower_C = actPower_C;
        ReaPower_C = reaPower_C;
        AppPower_C = appPower_C;
        ActPower_Sum = actPower_Sum;
        ReaPower_Sum = reaPower_Sum;
        AppPower_Sum = appPower_Sum;
        this.PF = PF;
        this.UnbalancedRate = UnbalancedRate;
    }

    public PowerBean() {
    }

    public static PowerBean of(int PMU_ID, long timestamp, long FlinkProcessTime, long FlinkDelayTime, double actPower_A, double reaPower_A, double appPower_A, double actPower_B, double reaPower_B, double appPower_B, double actPower_C, double reaPower_C, double appPower_C, double actPower_Sum, double reaPower_Sum, double appPower_Sum, double PF, double UnbalancedRate) {
        return new PowerBean(PMU_ID, timestamp, FlinkProcessTime, FlinkDelayTime, actPower_A, reaPower_A, appPower_A, actPower_B, reaPower_B, appPower_B, actPower_C, reaPower_C, appPower_C, actPower_Sum, reaPower_Sum, appPower_Sum, PF, UnbalancedRate);

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