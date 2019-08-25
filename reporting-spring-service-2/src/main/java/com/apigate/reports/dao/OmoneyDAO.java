package com.apigate.reports.dao;

import com.apigate.reports.dto.OmoneyStatusDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class OmoneyDAO {

    private static final Logger LOGGER = Logger.getLogger(OmoneyDAO.class.getName());
    private static final String GET_COUNT_QUERY = "SELECT COUNT(*) FROM OMONEY_STAT WHERE LOGGED_TIME < ? AND LOGGED_TIME > ?";
    private static final String GET_STATUS_QUERY = "SELECT * FROM OMONEY_STAT WHERE LOGGED_TIME < ? AND LOGGED_TIME > ? ";
    private static final String GET_SUMMARY_QUERY = "SELECT SP_NAME, APP_NAME, MERCHANT_KEY, CURRENCY, SUM(AMOUNT) from omoney_stat where STATUS='SUCCESS' AND LOGGED_TIME < '2019-06-12' AND LOGGED_TIME > '2019-01-12'group by SP_NAME, APP_NAME, MERCHANT_KEY, CURRENCY";

    @Value("${mysql.url}")
    private String mysqlurl;

    @Value("${mysql.uname}")
    private String mysqluname;

    @Value("${mysql.password}")
    private String mysqlpassword;


    public List<OmoneyStatusDTO> getResultSet(String upperLimit, String lowerLimit){
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        List<OmoneyStatusDTO> response = new ArrayList<>();

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(mysqlurl, mysqluname, mysqlpassword);
            ps = connection.prepareStatement(GET_STATUS_QUERY, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setString(1,upperLimit);
            ps.setString(2,lowerLimit);
            ps.setFetchSize(Integer.MIN_VALUE);   // ??????????

            resultSet = ps.executeQuery();

            while (resultSet.next()) {

                OmoneyStatusDTO rowStat = new OmoneyStatusDTO();
                rowStat.setId(resultSet.getString(1));
                rowStat.setOrderId(resultSet.getString(2));
                rowStat.setCurency(resultSet.getString(3));
                rowStat.setAmount(resultSet.getString(4));
                rowStat.setTime(resultSet.getString(5));
                rowStat.setStatus(resultSet.getString(6));
                rowStat.setSpName(resultSet.getString(7));
                rowStat.setAppName(resultSet.getString(8));
                rowStat.setMerchantKey(resultSet.getString(9));

                response.add(rowStat);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try{
                if (resultSet != null)
                    resultSet.close();
                if (ps != null)
                    ps.close();
                if (connection != null)
                    connection.close();

            }catch (SQLException logOrIgnore) {
                LOGGER.severe(logOrIgnore.getMessage());
            }
        }

        return response;
    }

    public int getResultCount(String upperLimit, String lowerLimit){
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        int rowCount = 0;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(mysqlurl, mysqluname, mysqlpassword);
            ps = connection.prepareStatement(GET_COUNT_QUERY, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setString(1,upperLimit);
            ps.setString(2,lowerLimit);
            ps.setFetchSize(Integer.MIN_VALUE);   // ??????????
            resultSet = ps.executeQuery();

            while (resultSet.next()) {
                rowCount = resultSet.getInt("count(*)");
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try{
                if (resultSet != null)
                    resultSet.close();
                if (ps != null)
                    ps.close();
                if (connection != null)
                    connection.close();

            }catch (SQLException logOrIgnore) {
                LOGGER.severe(logOrIgnore.getMessage());
            }
        }

        return rowCount;
    }
}
