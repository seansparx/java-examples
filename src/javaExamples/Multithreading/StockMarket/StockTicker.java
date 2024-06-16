package javaExamples.Multithreading.StockMarket;

public class StockTicker {
	
    public static void main(String[] args) {
    	
        // Names of the stock exchanges
        String[] exchanges = {"NYSE", "NASDAQ", "LSE", "JPX", "SSE"};

        // Create and start a thread for each stock exchange
        for (String exchange : exchanges) {
        	
            Thread thread = new Thread(new StockExchange(exchange));
            thread.start();
        }
    }
}
