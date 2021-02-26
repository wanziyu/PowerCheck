package bean;

public class SourceBean implements PmuIdBean {
    public int PMU_ID;
    public long Timestamp;

    public double Mag_VA1;//Mag VA1 [V]
    public double Phase_VA1;// Phase VA1 [rad]

    public double Mag_VB1;// Mag VB1 [V]
    public double Phase_VB1;// Phase VB1 [rad]

    public double Mag_VC1;// Mag VC1 [V]
    public double Phase_VC1;// Phase VC1 [rad]

    public double Mag_IA1;// Mag IA1 [A]
    public double Phase_IA1;// Phase IA1 [rad]

    public double Mag_IB1;// Mag IB1 [A]
    public double Phase_IB1;// Phase IB1 [rad]

    public double Mag_IC1;// Mag IC1 [A]
    public double Phase_IC1;// Phase IC1 [rad]

    public SourceBean(int PMU_ID, long Timestamp, double mag_VA1, double phase_VA1, double mag_VB1, double phase_VB1,
                      double mag_VC1, double phase_VC1, double mag_IA1, double phase_IA1, double mag_IB1, double phase_IB1,
                      double mag_IC1, double phase_IC1) {
        this.PMU_ID = PMU_ID;
        this.Timestamp = Timestamp;
        Mag_VA1 = mag_VA1;
        Phase_VA1 = phase_VA1;
        Mag_VB1 = mag_VB1;
        Phase_VB1 = phase_VB1;
        Mag_VC1 = mag_VC1;
        Phase_VC1 = phase_VC1;
        Mag_IA1 = mag_IA1;
        Phase_IA1 = phase_IA1;
        Mag_IB1 = mag_IB1;
        Phase_IB1 = phase_IB1;
        Mag_IC1 = mag_IC1;
        Phase_IC1 = phase_IC1;
    }

    public SourceBean() {
    }



    public static SourceBean of(int PMU_ID, long Timestamp, double mag_VA1, double phase_VA1, double mag_VB1, double phase_VB1,
                                double mag_VC1, double phase_VC1, double mag_IA1, double phase_IA1, double mag_IB1, double phase_IB1,
                                double mag_IC1, double phase_IC1) {
        return new SourceBean(PMU_ID, Timestamp, mag_VA1, phase_VA1, mag_VB1, phase_VB1,
                mag_VC1, phase_VC1, mag_IA1, phase_IA1, mag_IB1, phase_IB1,
                mag_IC1, phase_IC1);
    }


    @Override
    public int getPmuId() {
        return this.PMU_ID;
    }
}
