package javaExamples.Multithreading.WebScraper;

public class MultithreadedWebScraper {
	
    public static void main(String[] args) {
    	
        String[] urls = {
            "https://www.google.com",
            "https://www.wikipedia.org",
            "https://www.github.com",
            "https://www.stackoverflow.com",
            "https://www.reddit.com"
        };

        for (String url : urls) {
        	
            Thread thread = new Thread(new WebScraper(url));
            thread.start();
        }
    }
}
