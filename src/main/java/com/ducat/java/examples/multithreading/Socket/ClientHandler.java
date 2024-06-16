package com.ducat.java.examples.multithreading.Socket;

import java.io.*;
import java.net.*;

public class ClientHandler extends Thread {
	
    private final Socket socket;

    public ClientHandler(Socket socket) {
    	
        this.socket = socket;
    }

    @Override
    public void run() {
    	
        try (InputStream input = socket.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(input));
             OutputStream output = socket.getOutputStream();
             PrintWriter writer = new PrintWriter(output, true)) {

            String clientMessage;
            
            while ((clientMessage = reader.readLine()) != null) {
            	
                System.out.println("Received: " + clientMessage);
                writer.println("Server response: " + clientMessage);

                if (clientMessage.equalsIgnoreCase("exit")) {
                    break;
                }
            }

            socket.close();
            System.out.println("Client disconnected");

        } 
        catch (IOException ex) {
        	
            System.out.println("Server exception: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
