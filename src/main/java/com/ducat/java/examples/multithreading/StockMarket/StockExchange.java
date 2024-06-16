package com.ducat.java.examples.multithreading.StockMarket;

import java.util.Random;

public class StockExchange implements Runnable {
	
    private final String exchangeName;

    public StockExchange(String exchangeName) {
    	
        this.exchangeName = exchangeName;
    }

    @Override
    public void run() {
    	
        Random random = new Random();
        
        try {
            
            while (true) {
            	
                // Simulate getting a new stock price every second
                int stockPrice = 100 + random.nextInt(50);  // Random stock price between 100 and 150
                System.out.println(exchangeName + " - Stock Price: $" + stockPrice);
                // Sleep for 1 second to simulate real-time data fetching
                Thread.sleep(100);
            }
            
        } 
        catch (InterruptedException e) {
        	
            System.out.println(exchangeName + " - Thread interrupted");
        }
    }
}
