package com.ducat.java.examples.multithreading.ETL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class DataTransferTask implements Runnable {
    private final Connection sourceConnection;
    private final Connection targetConnection;
    private final String tableName;
    private final List<String> columnNames;

    public DataTransferTask(Connection sourceConnection, Connection targetConnection, String databaseName, String tableName) throws SQLException {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
        this.tableName = tableName;
        this.columnNames = DatabaseUtils.getColumnNames(sourceConnection, databaseName, tableName);
    }

    @Override
    public void run() {
        String createTableSQL = buildCreateTableSQL();
        String selectSQL = buildSelectSQL();
        String insertSQL = buildInsertSQL();

        try {
            DatabaseUtils.createTableIfNotExists(targetConnection, createTableSQL);

            try (PreparedStatement selectStmt = sourceConnection.prepareStatement(selectSQL);
                 ResultSet resultSet = selectStmt.executeQuery();
                 PreparedStatement insertStmt = targetConnection.prepareStatement(insertSQL)) {

                while (resultSet.next()) {
                    for (int i = 0; i < columnNames.size(); i++) {
                        insertStmt.setObject(i + 1, resultSet.getObject(columnNames.get(i)));
                    }
                    insertStmt.addBatch();
                }
                insertStmt.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String buildCreateTableSQL() {
        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        createTableSQL.append(tableName).append(" (");

        for (int i = 0; i < columnNames.size(); i++) {
            createTableSQL.append(columnNames.get(i)).append(" VARCHAR(255)");
            if (i < columnNames.size() - 1) {
                createTableSQL.append(", ");
            }
        }
        createTableSQL.append(");");
        return createTableSQL.toString();
    }

    private String buildSelectSQL() {
        return "SELECT " + String.join(", ", columnNames) + " FROM " + tableName;
    }

    private String buildInsertSQL() {
        String columns = String.join(", ", columnNames);
        String placeholders = String.join(", ", columnNames.stream().map(c -> "?").toArray(String[]::new));
        return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
    }
}
