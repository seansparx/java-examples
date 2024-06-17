package com.ducat.java.examples.multithreading.ETL;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseSync {
    
    private static final String SOURCE_DB_URL = "jdbc:mysql://localhost:3306/source_db";
    private static final String TARGET_DB_URL = "jdbc:mysql://localhost:3306/target_db";
    private static final String USER = "root";
    private static final String PASSWORD = "Sparx@123";
    private static final int THREAD_POOL_SIZE = 10;

    public static void main(String[] args) {
        
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        
        Connection sourceConnection;
        Connection targetConnection;

        try {
            
            sourceConnection = DatabaseUtils.getConnection(SOURCE_DB_URL, USER, PASSWORD);
            targetConnection = DatabaseUtils.getConnection(TARGET_DB_URL, USER, PASSWORD);

            // Specify the database name and table name you want to fetch columns for
            String sourceDatabase = "source_db";
            String targetDatabase = "target_db";
            
            // Run the schema synchronization task
            SchemaSyncTask schemaSyncTask = new SchemaSyncTask(sourceConnection, targetConnection, sourceDatabase, targetDatabase);
            executorService.submit(schemaSyncTask);
            
            // Synchronize data transfer for each table
            List<String> tables = DatabaseUtils.getTableNames(sourceConnection, sourceDatabase);

            for (String table : tables) {
                
                DataTransferTask task = new DataTransferTask(sourceConnection, targetConnection, sourceDatabase, table);
                executorService.submit(task);
            }
        } 
        catch (SQLException e) {
            e.printStackTrace();
        }
        
        finally {
            
//            if (sourceConnection != null) {
//                
//                try {
//                    sourceConnection.close();
//                } 
//                catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//            
//            if (targetConnection != null) {
//                
//                try {
//                    targetConnection.close();
//                } 
//                catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
            
            executorService.shutdown();
        }
    }
}
