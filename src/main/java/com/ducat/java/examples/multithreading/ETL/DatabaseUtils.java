package com.ducat.java.examples.multithreading.ETL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author rakesh
 */
public class DatabaseUtils {
    
    public static Connection getConnection(String url, String user, String password) throws SQLException {
        
        return DriverManager.getConnection(url, user, password);
    }
}
