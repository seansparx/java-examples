package com.ducat.java.examples.multithreading.ETL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author rakesh
 */
public class DataTransferTask implements Runnable {
    
    private final Connection sourceConnection;
    private final Connection targetConnection;
    private final String tableName;

    public DataTransferTask(Connection sourceConnection, Connection targetConnection, String tableName) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
        this.tableName = tableName;
    }

    @Override
    public void run() {
        String selectSQL = "SELECT * FROM " + tableName;
        String insertSQL = "INSERT INTO " + tableName + " VALUES (?, ?, ?)"; // Adjust columns as needed

        try (PreparedStatement selectStmt = sourceConnection.prepareStatement(selectSQL);
             ResultSet resultSet = selectStmt.executeQuery();
             PreparedStatement insertStmt = targetConnection.prepareStatement(insertSQL)) {

            while (resultSet.next()) {
                // Assuming three columns: id, name, value; adjust as needed
                insertStmt.setInt(1, resultSet.getInt("id"));
                insertStmt.setString(2, resultSet.getString("name"));
                insertStmt.setDouble(3, resultSet.getDouble("value"));
                insertStmt.addBatch();
            }

            insertStmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
