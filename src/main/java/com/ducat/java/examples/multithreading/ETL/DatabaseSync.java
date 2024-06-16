package com.ducat.java.examples.multithreading.ETL;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author rakesh
 */
public class DatabaseSync {
    private static final String SOURCE_DB_URL = "jdbc:mysql://localhost:3306/source_db";
    private static final String TARGET_DB_URL = "jdbc:mysql://localhost:3306/target_db";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final int THREAD_POOL_SIZE = 5;
    private static final String[] TABLES = {"table1", "table2", "table3"}; // List of tables to sync

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        try (Connection sourceConnection = DatabaseUtils.getConnection(SOURCE_DB_URL, USER, PASSWORD);
             Connection targetConnection = DatabaseUtils.getConnection(TARGET_DB_URL, USER, PASSWORD)) {

            for (String table : TABLES) {
                DataTransferTask task = new DataTransferTask(sourceConnection, targetConnection, table);
                executorService.submit(task);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }
}
