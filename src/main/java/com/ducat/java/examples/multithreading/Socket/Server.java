package com.ducat.java.examples.multithreading.Socket;

import java.io.*;
import java.net.*;

public class Server {
	
    public static void main(String[] args) {
    	
        int port = 8080;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
        	
            System.out.println("Server is listening on port " + port);

            while (true) {
            	
                Socket socket = serverSocket.accept();
                System.out.println("New client connected");

                // Create a new thread to handle the client request
                new ClientHandler(socket).start();
            }

        } 
        catch (IOException ex) {
        	
            System.out.println("Server exception: " + ex.getMessage());
        }
    }
}
