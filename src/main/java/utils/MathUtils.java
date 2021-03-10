package utils;

import bean.PowerBean;
import bean.SourceBean;

import java.util.Arrays;
import java.util.Date;

public class MathUtils {

    private static double[] calAppPower(SourceBean bean) {
        double[] array = {
                bean.Mag_VA1 * bean.Mag_IA1,
                bean.Mag_VB1 * bean.Mag_IB1,
                bean.Mag_VC1 * bean.Mag_IC1
        };
        return array;
    }

    private static double[] calRadDiff(SourceBean bean) {
        double[] array = {
                bean.Phase_VA1 - bean.Phase_IA1,
                bean.Phase_VB1 - bean.Phase_IB1,
                bean.Phase_VC1 - bean.Phase_IC1
        };
        return array;
    }

    private static double[] calActPower(double[] appPower, double[] radDiff) {
        double[] actPower = {
                appPower[0] * Math.cos(radDiff[0]),
                appPower[1] * Math.cos(radDiff[1]),
                appPower[2] * Math.cos(radDiff[2])
        };
        return actPower;
    }

    private static double[] calReaPower(double[] appPower, double[] radDiff) {
        double[] reaPower = {
                appPower[0] * Math.sin(radDiff[0]),
                appPower[1] * Math.sin(radDiff[1]),
                appPower[2] * Math.sin(radDiff[2])
        };
        return reaPower;
    }


    public static PowerBean sourceToPower(SourceBean bean) {
        double[] appPower = calAppPower(bean);
        double[] radDiff = calRadDiff(bean);
        double[] actPower = calActPower(appPower, radDiff);
        double[] reaPower = calReaPower(appPower, radDiff);

        double appPowerSum = appPower[0] + appPower[1] + appPower[2];
        double actPowerSum = actPower[0] + actPower[1] + actPower[2];
        double reaPowerSum = reaPower[0] + reaPower[1] + reaPower[2];
        double PF = actPowerSum / appPowerSum;
        long flinkProcessTime = System.currentTimeMillis();
        long flinkDelayTime = flinkProcessTime - bean.Timestamp;
        double unbalanceRate = calUnbalanceRate(bean);

        return PowerBean.of(bean.PMU_ID, bean.Timestamp, flinkProcessTime, flinkDelayTime,
                actPower[0], reaPower[0], appPower[0],
                actPower[1], reaPower[1], appPower[1], actPower[2], reaPower[2], appPower[2],
                actPowerSum, reaPowerSum, appPowerSum, PF, unbalanceRate
        );
    }

    private static double calUnbalanceRate(SourceBean bean) {
        double V_average = bean.Mag_VA1 + bean.Mag_VB1 + bean.Mag_VC1;
        double tmpMax = Math.max(Math.abs(bean.Mag_VA1 - V_average), Math.abs(bean.Mag_VB1 - V_average));
        double totalMax = Math.max(tmpMax, Math.abs(bean.Mag_VC1 - V_average));
        return totalMax / V_average;
    }

}
