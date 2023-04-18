package it.damore.solr.importexport;

import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseWriter {
    private static Logger logger = LoggerFactory.getLogger(ClickHouseWriter.class);

    private static final String JDBC_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final String DB_URL = "jdbc:clickhouse://loclhost:8123/solrData";
    private static final String USER = "default";
    private static final String PASSWORD = "";

    public static void insertClickHouse(SolrDocumentList solrDocuments) {
        String tableName = "yfb_company_whole";
        String columnName1 = "company_complete";
        String columnName2 = "type";
        String columnName3 = "zhaobiaoCount";
        String columnName4 = "zhongbiaoCount";
        String columnName5 = "partyACompanyCount";
        String columnName6 = "partyBCompanyCount";
        String columnName7 = "relationCompanyCount";

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);
            // Open a connection
            connection = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            // Prepare statement
            String insertQuery = "INSERT INTO " + tableName + "(" + columnName1 + ", " + columnName2 + ", " + columnName3 +  ", " + columnName4 +  ", " + columnName5 +  ", " + columnName6 +  ", " + columnName7 + ") " +
                    "                                   VALUES ( ?, ?, ?, ?, ?, ?, ?)";
            preparedStatement = connection.prepareStatement(insertQuery);

            // 添加多个数据行
            for (int i = 0; i < solrDocuments.size(); i++) {
                preparedStatement.setString(1, (String) solrDocuments.get(i).get(columnName1));
                preparedStatement.setInt(2, (Integer) solrDocuments.get(i).get(columnName2));
                preparedStatement.setInt(3, (Integer) solrDocuments.get(i).get(columnName3));
                preparedStatement.setInt(4, (Integer) solrDocuments.get(i).get(columnName4));
                preparedStatement.setInt(5, (Integer) solrDocuments.get(i).get(columnName5));
                preparedStatement.setInt(6, (Integer) solrDocuments.get(i).get(columnName6));
                preparedStatement.setInt(7, (Integer) solrDocuments.get(i).get(columnName7));
                preparedStatement.addBatch();
            }
            int[] result = preparedStatement.executeBatch();
            logger.info("成功写入："+result.length);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Close resources
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
