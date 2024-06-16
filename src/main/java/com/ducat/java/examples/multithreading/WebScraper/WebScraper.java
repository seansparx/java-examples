package com.ducat.java.examples.multithreading.WebScraper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class WebScraper implements Runnable {
	
    private final String urlString;

    public WebScraper(String urlString) {
    	
        this.urlString = urlString;
    }

    @Override
    public void run() {
    	
        try {
        	
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            
            if (responseCode == 200) {
            	
                StringBuilder content;
                try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    String inputLine;
                    content = new StringBuilder();
                    while ((inputLine = in.readLine()) != null) {
                        content.append(inputLine);
                    }
                }
                connection.disconnect();

                System.out.println("Data from " + urlString + ":");
                System.out.println(content.substring(0, Math.min(content.length(), 200))); // Print first 200 characters
            } 
            else {
                System.out.println("Failed to fetch data from " + urlString);
            }
        } 
        catch (IOException e) {
            System.out.println("Error fetching data from " + urlString + ": " + e.getMessage());
        }
    }
}
