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

    
    public static List<ColumnInfo> getColumnNames(Connection connection, String databaseName, String tableName) throws SQLException {
        
        List<ColumnInfo> columnNames = new ArrayList<>();        
        DatabaseMetaData metaData = connection.getMetaData();
        
        try (ResultSet columns = metaData.getColumns(databaseName, null, tableName, "%")) {
                        
            while (columns.next()) {
                
                String columnName = columns.getString("COLUMN_NAME");
                String dataType = columns.getString("TYPE_NAME");
                int columnSize = columns.getInt("COLUMN_SIZE");
                
                columnNames.add(new ColumnInfo(columnName, dataType, columnSize));
            }
        }
        
        return columnNames;
    }
    
    
    public static void createTableIfNotExists(Connection connection, String createTableSQL) throws SQLException {
        
        try (Statement statement = connection.createStatement()) {
            
            statement.executeUpdate(createTableSQL);
        }
    }
    
    
    public static class ColumnInfo {
        
        private final String columnName;
        private final String dataType;
        private final int columnSize;

        public ColumnInfo(String columnName, String dataType, int columnSize) {
            
            this.columnName = columnName;
            this.dataType = dataType;
            this.columnSize = columnSize;
        }

        public String getColumnName() {
            
            return columnName;
        }

        public String getDataType() {
            
            return dataType;
        }
        
        public int getColumnSize() {
            
            return columnSize;
        }
        
        @Override
        public String toString() {
            
            return columnName + " " + dataType + "(" + columnSize + ")";
        }
    }
}
