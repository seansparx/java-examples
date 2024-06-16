package com.ducat.java.examples.multithreading.ETL;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseUtils {

    public static Connection getConnection(String url, String user, String password) throws SQLException {
        
        return DriverManager.getConnection(url, user, password);
    }

    
    public static List<String> getTableNames(Connection connection, String databaseName) throws SQLException {
        
        List<String> tableNames = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        
        try (ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {
            
            while (tables.next()) {
                
                tableNames.add(tables.getString("TABLE_NAME"));
            }
        }
        
        return tableNames;
    }

    
    public static List<String> getColumnNames(Connection connection, String databaseName, String tableName) throws SQLException {
        
        List<String> columnNames = new ArrayList<>();        
        DatabaseMetaData metaData = connection.getMetaData();
        
        try (ResultSet columns = metaData.getColumns(databaseName, null, tableName, "%")) {
            
            while (columns.next()) {
                
                columnNames.add(columns.getString("COLUMN_NAME"));
            }
        }
        
        return columnNames;
    }
    
    
    public static void createTableIfNotExists(Connection connection, String createTableSQL) throws SQLException {
        
        try (Statement statement = connection.createStatement()) {
            
            statement.executeUpdate(createTableSQL);
        }
    }
}
