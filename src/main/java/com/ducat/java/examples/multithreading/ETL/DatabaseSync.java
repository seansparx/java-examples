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
    private static final int THREAD_POOL_SIZE = 5;

    public static void main(String[] args) {
        
        Connection sourceConnection = null;
        Connection targetConnection = null;
        
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        try {
            
            sourceConnection = DatabaseUtils.getConnection(SOURCE_DB_URL, USER, PASSWORD);
            targetConnection = DatabaseUtils.getConnection(TARGET_DB_URL, USER, PASSWORD);

            // Specify the database name and table name you want to fetch columns for
            String databaseName = "source_db";
            List<String> tables = DatabaseUtils.getTableNames(sourceConnection, databaseName);

            for (String table : tables) {
                DataTransferTask task = new DataTransferTask(sourceConnection, targetConnection, databaseName, table);
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
