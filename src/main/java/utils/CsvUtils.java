package utils;

import com.alibaba.fastjson.JSON;
import bean.SourceBean;

import java.io.*;


public class CsvUtils {

    public static SourceBean originalLineToBean(int PMU_ID, String line) {
        String[] fields = line.split(",");
//        long SOC = Long.parseLong(fields[0]);
//        int FRACSEC = Integer.parseInt(fields[1]);
        long Timestamp = Long.parseLong(fields[0] + fields[1]);

        double Mag_VA1 = Double.parseDouble(fields[6]);
        double Phase_VA1 = Double.parseDouble(fields[7]);
        double Mag_VB1 = Double.parseDouble(fields[10]);
        double Phase_VB1 = Double.parseDouble(fields[11]);
        double Mag_VC1 = Double.parseDouble(fields[14]);
        double Phase_VC1 = Double.parseDouble(fields[15]);

        double Mag_IA1 = Double.parseDouble(fields[18]);
        double Phase_IA1 = Double.parseDouble(fields[19]);
        double Mag_IB1 = Double.parseDouble(fields[20]);
        double Phase_IB1 = Double.parseDouble(fields[21]);
        double Mag_IC1 = Double.parseDouble(fields[22]);
        double Phase_IC1 = Double.parseDouble(fields[23]);
        return SourceBean.of(PMU_ID, Timestamp, Mag_VA1, Phase_VA1, Mag_VB1, Phase_VB1, Mag_VC1, Phase_VC1,
                Mag_IA1, Phase_IA1, Mag_IB1, Phase_IB1, Mag_IC1, Phase_IC1);
    }

    public static void main(String[] args) throws Exception {
        String originalFilePath = "src/main/resources/2019-03-23_00h_UTC_PMUID02.CSV";
        String txtFilePath = "src/main/resources/2019-03-23_00h_UTC_PMUID02.txt";
        BufferedReader reader = new BufferedReader(new FileReader(originalFilePath));
        BufferedWriter writer = new BufferedWriter(new FileWriter(txtFilePath));
        String line = reader.readLine();
        SourceBean sourceBean;
        while ((line = reader.readLine()) != null) {
            sourceBean = originalLineToBean(-1, line);
            String jsonString = JSON.toJSONString(sourceBean);
            writer.write(jsonString);
            writer.newLine();
        }
        reader.close();
        writer.close();
    }
}
