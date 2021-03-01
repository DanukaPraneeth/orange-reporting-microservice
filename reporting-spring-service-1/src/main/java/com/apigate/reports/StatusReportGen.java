package com.apigate.reports;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.logging.Logger;

@RestController
public class StatusReportGen {

    private final static Logger LOGGER = Logger.getLogger(StatusReportGen.class.getName());

    @Value("${mysql.url}")
    private String mysqlurl;

    @Value("${mysql.uname}")
    private String mysqluname;

    @Value("${mysql.password}")
    private String mysqlpassword;

    @Value("${carbonhome}")
    private String carbonHome;

    @Value("${mysql.maxrowcount}")
    private int maxrowcount;

    /**
     *
     * Sample curl request curl -X GET 'http://localhost:8080/generatecsv/status-report?upper=2019-07-30&lower=2019-06-30&reportName=Report1'
     *
     **/
    @RequestMapping(method = RequestMethod.GET, value = "/generatecsv/status-report")
    public boolean generatecsv(@RequestParam Map<String, String> queryParam, HttpServletRequest request) {

        LOGGER.info(queryParam.toString());

        String upperLimit = queryParam.get("upper");
        String lowerLimit = queryParam.get("lower");
        String reportName = queryParam.get("reportName");

        String countQuery = "SELECT COUNT(*) FROM OMONEY_STAT WHERE LOGGED_TIME < '"
                            + upperLimit + "' AND LOGGED_TIME > '" + lowerLimit + "'";
        String executeQuery = "SELECT * FROM OMONEY_STAT WHERE LOGGED_TIME < '"
                            + upperLimit + "' AND LOGGED_TIME > '" + lowerLimit + "'";

        int rowCount = 0;

        LOGGER.info(executeQuery);
        LOGGER.info(carbonHome + reportName);

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        ResultSet resultSetCount = null;

        BufferedWriter writer = null;
        boolean success = false;

        try {
            LOGGER.info("START Report Gen " + String.valueOf(System.currentTimeMillis()));
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(mysqlurl, mysqluname, mysqlpassword);
            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);

            resultSetCount = statement.executeQuery(countQuery);
            while (resultSetCount.next()) {
                rowCount = resultSetCount.getInt("count(*)");
            }
            LOGGER.info("Counted " + String.valueOf(System.currentTimeMillis()));

            int noOfFiles = rowCount / maxrowcount + 1;
            int count = 0;
            int fileNumber = 1;

            resultSet = statement.executeQuery(executeQuery);

            File fileTemp = new File(carbonHome + reportName + "-" + fileNumber + "-of-" + noOfFiles + ".csv.wte");
            File fileCsv = new File(carbonHome + reportName + "-" + fileNumber + "-of-" + noOfFiles + ".csv");
            fileTemp.getParentFile().mkdirs();

            writer = new BufferedWriter(new FileWriter(fileTemp));
            writer.write("From : ;");
            writer.write(lowerLimit + ";");
            writer.write("To : ;");
            writer.write(upperLimit);
            writer.write("\n");
            writer.write("Transaction ID;");
            writer.write("Service Provider;");
            writer.write("Application Name;");
            writer.write("Country Code;");
            writer.write("Operator Name;");
            writer.write("Merchant Key;");
            writer.write("Currency;");
            writer.write("Transaction Amount;");
            writer.write("Transaction Status;");
            writer.write("Transaction Time");
            writer.write("\n");

            while (resultSet.next()) {
                count++;

                if (resultSet.getString(2) != null) {
                    writer.write(resultSet.getString(2));
                }
                writer.write(';');
                if (resultSet.getString(7) != null) {
                    writer.write(resultSet.getString(7));
                }
                writer.write(';');
                if (resultSet.getString(8) != null) {
                    writer.write(resultSet.getString(8));
                }
                writer.write(';');
                if (resultSet.getString(9) != null) {
                    writer.write(resultSet.getString(9));
                }
                writer.write(';');
                if (resultSet.getString(10) != null) {
                    writer.write(resultSet.getString(10));
                }
                writer.write(';');
                if (resultSet.getString(11) != null) {
                    writer.write(resultSet.getString(11));
                }
                writer.write(';');
                if (resultSet.getString(3) != null) {
                    writer.write(resultSet.getString(3));
                }
                writer.write(';');
                if (resultSet.getString(4) != null) {
                    writer.write(resultSet.getString(4));
                }
                writer.write(';');
                if (resultSet.getString(6) != null) {
                    writer.write(resultSet.getString(6));
                }
                writer.write(';');
                if (resultSet.getString(5) != null) {
                    writer.write(resultSet.getString(5));
                }
                if (!resultSet.isLast()) {
                    writer.write("\n");
                }

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
                    writer.write("Transaction ID;");
                    writer.write("Service Provider;");
                    writer.write("Application Name;");
                    writer.write("Country Code;");
                    writer.write("Operator Name;");
                    writer.write("Merchant Key;");
                    writer.write("Currency;");
                    writer.write("Transaction Amount;");
                    writer.write("Transaction Status;");
                    writer.write("Transaction Time");
                    writer.write("\n");
                }
            }
            writer.flush();

            if (fileTemp.exists()) {
                success = fileTemp.renameTo(fileCsv);
            }

            LOGGER.info("END Report Gen " + String.valueOf(System.currentTimeMillis()));
        } catch (NullPointerException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    LOGGER.severe(e.getMessage());
                }
            }
            if (resultSet != null)
                try {
                    resultSet.close();
                } catch (SQLException logOrIgnore) {
                    LOGGER.severe(logOrIgnore.getMessage());
                }
            if (resultSetCount != null)
                try {
                    resultSetCount.close();
                } catch (SQLException logOrIgnore) {
                    LOGGER.severe(logOrIgnore.getMessage());
                }
            if (statement != null)
                try {
                    statement.close();

                } catch (SQLException logOrIgnore) {
                    LOGGER.severe(logOrIgnore.getMessage());
                }
            if (connection != null)
                try {
                    connection.close();
                } catch (SQLException logOrIgnore) {
                    LOGGER.severe(logOrIgnore.getMessage());
                }
        }

        return success;
    }


}
