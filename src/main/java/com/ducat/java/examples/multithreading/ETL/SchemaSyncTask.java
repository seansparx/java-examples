package com.ducat.java.examples.multithreading.ETL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaSyncTask implements Runnable {
    
    private final Connection sourceConnection;
    private final Connection targetConnection;
    private final String sourceDatabase;
    private final String targetDatabase;

    public SchemaSyncTask(Connection sourceConnection, Connection targetConnection, String sourceDatabase, String targetDatabase) {
        
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
        this.sourceDatabase = sourceDatabase;
        this.targetDatabase = targetDatabase;
    }

    @Override
    public void run() {
        
        try {
            
            Map<String, List<DatabaseUtils.ColumnInfo>> sourceSchema = DatabaseUtils.getDatabaseSchema(sourceConnection, sourceDatabase);
            Map<String, List<DatabaseUtils.ColumnInfo>> targetSchema = DatabaseUtils.getDatabaseSchema(targetConnection, targetDatabase);

            for (String tableName : sourceSchema.keySet()) {
                                
                if (!targetSchema.containsKey(tableName)) {
                    
                    // Table does not exist in target, create it
                    String createTableSQL = buildCreateTableSQL(tableName, sourceSchema.get(tableName));
                    DatabaseUtils.createTableIfNotExists(targetConnection, createTableSQL);
                } 
                else {
                    
                    // Table exists, check for column differences
                    List<DatabaseUtils.ColumnInfo> sourceColumns = sourceSchema.get(tableName);
                    List<DatabaseUtils.ColumnInfo> targetColumns = targetSchema.get(tableName);

                    for (DatabaseUtils.ColumnInfo sourceColumn : sourceColumns) {
                                                
                        String alterColumnSQL;
                        
                        // Column does not exist in target, add it.
                        if (!targetColumns.toString().contains(sourceColumn.getColumnName().trim())) {
                            
                            alterColumnSQL = buildAddColumnSQL(tableName, sourceColumn);
                        }
                        
                        // Column name exists in target, but something miss matched. alter it.
                        else {
                            
                            alterColumnSQL = buildAlterColumnSQL(tableName, sourceColumn);
                        }
                                                
                        try (PreparedStatement stmt = targetConnection.prepareStatement(alterColumnSQL)) {
                            stmt.execute();
                        }
                    }
                }
            }
        } 
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    
    private String buildCreateTableSQL(String tableName, List<DatabaseUtils.ColumnInfo> columnInfos) {
        
        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        createTableSQL.append(tableName).append(" (");

        List<String> primaryKeys = columnInfos.stream()
                .filter(DatabaseUtils.ColumnInfo::isPrimaryKey)
                .map(DatabaseUtils.ColumnInfo::getColumnName)
                .collect(Collectors.toList());

        for (int i = 0; i < columnInfos.size(); i++) {
            
            DatabaseUtils.ColumnInfo columnInfo = columnInfos.get(i);
            createTableSQL.append(columnInfo.getColumnName()).append(" ").append(columnInfo.getDataType());
            
            if (columnInfo.getDataType().equalsIgnoreCase("VARCHAR") 
                    || columnInfo.getDataType().equalsIgnoreCase("CHAR")
                        || columnInfo.getDataType().equalsIgnoreCase("INT")) {
                
                createTableSQL.append("(").append(columnInfo.getColumnSize()).append(")");
            }
            if (i < columnInfos.size() - 1) {
                createTableSQL.append(", ");
            }
        }

        if (!primaryKeys.isEmpty()) {
            
            createTableSQL.append(", PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
        }

        createTableSQL.append(");");
        
        return createTableSQL.toString();
    }

        
    private String buildAddColumnSQL(String tableName, DatabaseUtils.ColumnInfo columnInfo) {
        
        StringBuilder addColumnSQL = new StringBuilder("ALTER TABLE ");
        addColumnSQL.append(tableName).append(" ADD COLUMN ");
        addColumnSQL.append(columnInfo.getColumnName()).append(" ").append(columnInfo.getDataType());
        
        if (columnInfo.getDataType().equalsIgnoreCase("VARCHAR") 
                || columnInfo.getDataType().equalsIgnoreCase("CHAR")
                    || columnInfo.getDataType().equalsIgnoreCase("INT") ) {
            
            addColumnSQL.append("(").append(columnInfo.getColumnSize()).append(") NULL DEFAULT NULL");
        }
        
        return addColumnSQL.toString();
    }
    
    
    private String buildAlterColumnSQL(String tableName, DatabaseUtils.ColumnInfo columnInfo) {
        
        StringBuilder addColumnSQL = new StringBuilder("ALTER TABLE ");
        
        addColumnSQL.append(tableName)
                .append(" CHANGE ")
                .append(columnInfo.getColumnName())
                .append(" ");
        
        addColumnSQL.append(columnInfo.getColumnName()).append(" ").append(columnInfo.getDataType());
        
        if (columnInfo.getDataType().equalsIgnoreCase("VARCHAR") 
                || columnInfo.getDataType().equalsIgnoreCase("CHAR")
                    || columnInfo.getDataType().equalsIgnoreCase("INT") ) {
            
            addColumnSQL.append("(").append(columnInfo.getColumnSize()).append(") NULL DEFAULT NULL");
        }
        
        return addColumnSQL.toString();
        
        // ALTER TABLE `app_ads` ADD PRIMARY KEY(`id`);
        
        // ALTER TABLE `app_ads` CHANGE `id` `id` INT(9) NULL DEFAULT NULL;
        // ALTER TABLE `app_ads` CHANGE `id` `id` INT(10)
    }
}
