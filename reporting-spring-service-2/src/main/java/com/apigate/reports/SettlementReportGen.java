package com.apigate.reports;

import com.apigate.reports.dao.OmoneyDAO;
import com.apigate.reports.dto.OmoneyStatusDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class SettlementReportGen {

    private final static Logger LOGGER = Logger.getLogger(SettlementReportGen.class.getName());

    @Value("${carbonhome}")
    private String carbonHome;

    @Value("${mysql.maxrowcount}")
    private int maxrowcount;

    @Autowired
    private OmoneyDAO dbService;

    @RequestMapping(method = RequestMethod.GET, value = "/generatecsv/spReport")
    public boolean generatecsv(@RequestParam Map<String, String> queryParam, HttpServletRequest request) {

        LOGGER.info(queryParam.toString());

        String spName = queryParam.get("sp");
        String upperLimit = queryParam.get("upper");
        String lowerLimit = queryParam.get("lower");
        String reportName = queryParam.get("reportName");

        if(upperLimit.isEmpty() || lowerLimit.isEmpty()){
            LOGGER.info("Empty input parameters in the request");
            return false;
        }

        BufferedWriter writer = null;
        boolean success = false;

        try {
            LOGGER.info("START Report Gen " + String.valueOf(System.currentTimeMillis()));

            int rowCount = dbService.getResultCount(upperLimit, lowerLimit);
            LOGGER.info("Counted " + String.valueOf(System.currentTimeMillis()));

            if(rowCount < 1){
                LOGGER.info("Row count less than 1");
                return false;
            }

            int noOfFiles = rowCount / maxrowcount + 1;
            int count = 0;
            int fileNumber = 1;

            List<OmoneyStatusDTO> statList = dbService.getResultSet(upperLimit, lowerLimit);

            File fileTemp = new File(carbonHome + reportName + "-" + fileNumber + "-of-" + noOfFiles + ".csv.wte");
            File fileCsv = new File(carbonHome + reportName + "-" + fileNumber + "-of-" + noOfFiles + ".csv");
            fileTemp.getParentFile().mkdirs();

            writer = new BufferedWriter(new FileWriter(fileTemp));
            writer.write("Transaction ID");
            writer.write(',');
            writer.write("Service Provider");
            writer.write(',');
            writer.write("Application Name");
            writer.write(',');
            writer.write("Operator Name");
            writer.write(',');
            writer.write("Currency");
            writer.write(',');
            writer.write("Transaction Amount");
            writer.write(',');
            writer.write("Transaction Status");
            writer.write(',');
            writer.write("Payment Time by SP");
            writer.write("\n");

            for (OmoneyStatusDTO rs : statList) {

                count++;

                if (rs.getOrderId() != null) {
                    writer.write(rs.getOrderId());
                }
                writer.write(',');
                if (rs.getSpName() != null) {
                    writer.write(rs.getSpName());
                }
                writer.write(',');
                if (rs.getAppName() != null) {
                    writer.write(rs.getAppName());
                }
                writer.write(',');
                if (rs.getMerchantKey() != null) {
                    writer.write(rs.getMerchantKey());
                }
                writer.write(',');
                if (rs.getCurency() != null) {
                    writer.write(rs.getCurency());
                }
                writer.write(',');
                if (rs.getAppName() != null) {
                    writer.write(rs.getAmount());
                }
                writer.write(',');
                if (rs.getStatus() != null) {
                    writer.write(rs.getStatus());
                }
                writer.write(',');
                if (rs.getTime() != null) {
                    writer.write(rs.getTime());
                }
                writer.write("\n");


                if (count % maxrowcount == 0) {
                    writer.flush();

                    if (fileTemp.exists()) {
                        success = fileTemp.renameTo(fileCsv);
                    }
                    fileNumber++;
                    fileTemp = new File(carbonHome + reportName + "-" + fileNumber + "-of-" + noOfFiles + ".csv.wte");
                    fileCsv = new File(carbonHome + reportName + "-" + fileNumber + "-of-" + noOfFiles + ".csv");
                    fileTemp.getParentFile().mkdirs();
                    writer = new BufferedWriter(new FileWriter(fileTemp));
                }
            }
            writer.flush();

            if (fileTemp.exists()) {
                success = fileTemp.renameTo(fileCsv);
            }

            LOGGER.info("END Report Gen " + String.valueOf(System.currentTimeMillis()));
        } catch (NullPointerException | IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    LOGGER.severe(e.getMessage());
                }
            }
        }

        return success;
    }
}
