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
public class SettlementReportGen {

    private final static Logger LOGGER = Logger.getLogger(SettlementReportGen.class.getName());

    @Value("${mysql.url}")
    private String mysqlurl;

    @Value("${mysql.uname}")
    private String mysqluname;

    @Value("${mysql.password}")
    private String mysqlpassword;

    @Value("${carbonhome}")
    private String carbonHome;

    /**
     *
     * Sample curl request curl -X GET 'http://localhost:8080/generatecsv/sp-report?upper=2019-07-30&lower=2019-06-30&reportName=Bizao'
     *
     **/
    @RequestMapping(method = RequestMethod.GET, value = "/generatecsv/sp-report")
    public boolean generateSpSettlementReport(@RequestParam Map<String, String> queryParam, HttpServletRequest request) {

        LOGGER.info(queryParam.toString());

        String upperLimit = queryParam.get("upper");
        String lowerLimit = queryParam.get("lower");
        String reportName = queryParam.get("reportName");

        String executeQuery = "SELECT SP_NAME, APP_NAME, COUNTRY_CODE, OPERATOR_NAME, MERCHANT_KEY, CURRENCY, SUM(AMOUNT) FROM omoney_stat WHERE " +
                                "STATUS='SUCCESS' AND LOGGED_TIME < '" + upperLimit + "' AND LOGGED_TIME > '" + lowerLimit + "' " +
                                "GROUP BY SP_NAME, APP_NAME, COUNTRY_CODE, OPERATOR_NAME, MERCHANT_KEY, CURRENCY";

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

            resultSet = statement.executeQuery(executeQuery);

            File fileCsv = new File(carbonHome + reportName + "-SP-Settlement-report.csv");
            fileCsv.getParentFile().mkdirs();

            writer = new BufferedWriter(new FileWriter(fileCsv));
            writer.write("From : ;");
            writer.write(lowerLimit + ";");
            writer.write("To : ;");
            writer.write(upperLimit);
            writer.write("\n");
            writer.write("Service Provider;");
            writer.write("Application Name;");
            writer.write("Country Code;");
            writer.write("Operator Name;");
            writer.write("Merchant Key;");
            writer.write("Currency;");
            writer.write("Gross Revenue");
            writer.write("\n");

            while (resultSet.next()) {

                if (resultSet.getString(1) != null) {
                    writer.write(resultSet.getString(1));
                }
                writer.write(';');
                if (resultSet.getString(2) != null) {
                    writer.write(resultSet.getString(2));
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
                if (resultSet.getString(5) != null) {
                    writer.write(resultSet.getString(5));
                }
                writer.write(';');
                if (resultSet.getString(6) != null) {
                    writer.write(resultSet.getString(6));
                }
                writer.write(';');
                if (resultSet.getString(7) != null) {
                    writer.write(resultSet.getString(7));
                }
                if (!resultSet.isLast()) {
                    writer.write("\n");
                }
            }
            writer.flush();
            success = true;

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


    /**
     *
     * Sample curl request curl -X GET 'http://localhost:8080/generatecsv/operator-report?upper=2019-07-30&lower=2019-06-30&reportName=Bizao'
     *
     **/
    @RequestMapping(method = RequestMethod.GET, value = "/generatecsv/operator-report")
    public boolean generateOperatorSettlementReport(@RequestParam Map<String, String> queryParam, HttpServletRequest request) {

        LOGGER.info(queryParam.toString());

        String upperLimit = queryParam.get("upper");
        String lowerLimit = queryParam.get("lower");
        String reportName = queryParam.get("reportName");

        String executeQuery = "SELECT COUNTRY_CODE, OPERATOR_NAME, MERCHANT_KEY, SP_NAME, APP_NAME, CURRENCY, SUM(AMOUNT) FROM omoney_stat WHERE " +
                "STATUS='SUCCESS' AND LOGGED_TIME < '" + upperLimit + "' AND LOGGED_TIME > '" + lowerLimit + "' " +
                "GROUP BY COUNTRY_CODE, OPERATOR_NAME, MERCHANT_KEY, SP_NAME, APP_NAME, CURRENCY";

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

            resultSet = statement.executeQuery(executeQuery);

            File fileCsv = new File(carbonHome + reportName + "-Operator-Settlement-report.csv");
            fileCsv.getParentFile().mkdirs();

            writer = new BufferedWriter(new FileWriter(fileCsv));
            writer.write("From : ;");
            writer.write(lowerLimit + ";");
            writer.write("To : ;");
            writer.write(upperLimit);
            writer.write("\n");
            writer.write("Country Code;");
            writer.write("Operator Name;");
            writer.write("Merchant Key;");
            writer.write("Service Provide;");
            writer.write("Application Name;");
            writer.write("Currency;");
            writer.write("Gross Revenue");
            writer.write("\n");

            while (resultSet.next()) {

                if (resultSet.getString(1) != null) {
                    writer.write(resultSet.getString(1));
                }
                writer.write(';');
                if (resultSet.getString(2) != null) {
                    writer.write(resultSet.getString(2));
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
                if (resultSet.getString(5) != null) {
                    writer.write(resultSet.getString(5));
                }
                writer.write(';');
                if (resultSet.getString(6) != null) {
                    writer.write(resultSet.getString(6));
                }
                writer.write(';');
                if (resultSet.getString(7) != null) {
                    writer.write(resultSet.getString(7));
                }
                if (!resultSet.isLast()) {
                    writer.write("\n");
                }
            }
            writer.flush();
            success=true;

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
