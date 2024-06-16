package com.ducat.java.examples.multithreading.ETL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DataTransferTask implements Runnable {
    
    private static final int CHUNK_SIZE = 100; // Number of rows to fetch and insert at a time.
    private static final int SLEEP_INTERVAL_MS = 10; // Sleep interval in milliseconds.
    
    private final Connection sourceConnection;
    private final Connection targetConnection;
    private final String tableName;
    private final List<DatabaseUtils.ColumnInfo> columnInfos;

    public DataTransferTask(Connection sourceConnection, Connection targetConnection, String databaseName, String tableName) throws SQLException {
        
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
        this.tableName = tableName;
        this.columnInfos = DatabaseUtils.getColumnNames(sourceConnection, databaseName, tableName);
    }

    @Override
    public void run() {
        
        String createTableSQL = buildCreateTableSQL();
        String selectSQL = buildSelectSQL();
        String insertSQL = buildInsertSQL();
        
        //System.out.print(createTableSQL);

        try {
            
            DatabaseUtils.createTableIfNotExists(targetConnection, createTableSQL);

            int offset = 0;
            boolean moreRows = true;
            
            while (moreRows) {
                
                try (PreparedStatement selectStmt = sourceConnection.prepareStatement(selectSQL + " LIMIT " + CHUNK_SIZE + " OFFSET " + offset);
                     ResultSet resultSet = selectStmt.executeQuery();
                     PreparedStatement insertStmt = targetConnection.prepareStatement(insertSQL)) {

                    int rowCount = 0;
                    
                    while (resultSet.next()) {
                        
                        rowCount++;
                        
                        for (int i = 0; i < columnInfos.size(); i++) {
                            
                            insertStmt.setObject(i + 1, resultSet.getObject(columnInfos.get(i).getColumnName()));
                        }
                        
                        insertStmt.addBatch();
                    }
                    
                    insertStmt.executeBatch();
                    moreRows = rowCount == CHUNK_SIZE;
                    offset += CHUNK_SIZE;
                    
                    // Sleep for a specified interval before processing the next chunk
                    if (moreRows) {
                        try {
                            Thread.sleep(SLEEP_INTERVAL_MS);
                        } catch (InterruptedException ex) {
                            Logger.getLogger(DataTransferTask.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                } 
                catch (SQLException e) {
                    e.printStackTrace();
                    break;
                }
            }
        } 
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String buildCreateTableSQL() {
        
        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        createTableSQL.append(tableName).append(" (");

        for (int i = 0; i < columnInfos.size(); i++) {
            
            DatabaseUtils.ColumnInfo columnInfo = columnInfos.get(i);
            createTableSQL.append(columnInfo.getColumnName()).append(" ").append(columnInfo.getDataType());
            
            if (columnInfo.getDataType().equalsIgnoreCase("VARCHAR") || columnInfo.getDataType().equalsIgnoreCase("CHAR")) {
                createTableSQL.append("(").append(columnInfo.getColumnSize()).append(")");
            }
            
            if (i < columnInfos.size() - 1) {
                createTableSQL.append(", ");
            }
        }
        
        createTableSQL.append(");");
        return createTableSQL.toString();
    }

    private String buildSelectSQL() {
        
        return "SELECT " + columnInfos.stream().map(DatabaseUtils.ColumnInfo::getColumnName).collect(Collectors.joining(", ")) + " FROM " + tableName;
    }

    private String buildInsertSQL() {
        
        String columns = columnInfos.stream().map(DatabaseUtils.ColumnInfo::getColumnName).collect(Collectors.joining(", "));
        String placeholders = columnInfos.stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
    }
}
