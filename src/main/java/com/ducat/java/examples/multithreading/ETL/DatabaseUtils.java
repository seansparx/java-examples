package com.ducat.java.examples.multithreading.ETL;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    
    
    public static Map<String, List<ColumnInfo>> getDatabaseSchema(Connection connection, String databaseName) throws SQLException {
        
        Map<String, List<ColumnInfo>> schema = new HashMap<>();
        List<String> tables = getTableNames(connection, databaseName);
        
        for (String table : tables) {
            schema.put(table, getColumnNamesWithDataTypesAndSizes(connection, databaseName, table));
        }
        
        return schema;
    }
    
    
    public static List<ColumnInfo> getColumnNamesWithDataTypesAndSizes(Connection connection, String databaseName, String tableName) throws SQLException {
        
        List<ColumnInfo> columns = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        
        // Fetch columns.
        try (ResultSet rs = metaData.getColumns(databaseName, null, tableName, null)) {
            
            while (rs.next()) {
                
                String columnName  = rs.getString("COLUMN_NAME");
                String dataType    = rs.getString("TYPE_NAME");
                int columnSize     = rs.getInt("COLUMN_SIZE");
                
                columns.add(new ColumnInfo(columnName, dataType, columnSize, false));
            }
        }
        
        // Fetch primary keys
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, null, tableName)) {
            
            while (rs.next()) {
                
                String columnName = rs.getString("COLUMN_NAME");
                
                for (ColumnInfo column : columns) {
                    
                    if (column.getColumnName().equals(columnName)) {
                        
                        column.setPrimaryKey(true);
                        break;
                    }
                }
            }
        }
        
        return columns;
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
        private boolean primaryKey;

        public ColumnInfo(String columnName, String dataType, int columnSize, boolean primaryKey) {
            
            this.columnName = columnName;
            this.dataType = dataType;
            this.columnSize = columnSize;
            this.primaryKey = primaryKey;
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
        
        public boolean isPrimaryKey() {
            
            return this.primaryKey;
        }

        public void setPrimaryKey(boolean primaryKey) {
            
            this.primaryKey = primaryKey;
        }
        
        @Override
        public String toString() {
            
            return columnName + " " + dataType + "(" + columnSize + ")";
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnInfo that = (ColumnInfo) o;
            return columnSize == that.columnSize &&
                    primaryKey == that.primaryKey &&
                    Objects.equals(columnName, that.columnName) &&
                    Objects.equals(dataType, that.dataType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, dataType, columnSize, primaryKey);
        }
    }
}
