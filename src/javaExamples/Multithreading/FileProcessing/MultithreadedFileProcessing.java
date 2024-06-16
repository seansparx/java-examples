package javaExamples.Multithreading.FileProcessing;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class MultithreadedFileProcessing {
	
    public static void main(String[] args) {
    	
        // Directory containing files to be processed
        File dir = new File("/home/spxlpt099/Documents/DUCAT/brouchers");

        // List to hold Future objects representing the results of file processing
        List<Future<Integer>> results = new ArrayList<>();

        // Create a thread pool with a fixed number of threads
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        // Submit FileProcessor tasks for each file in the directory
        for (File file : dir.listFiles()) {
        	
            if (file.isFile()) {
                FileProcessor fileProcessor = new FileProcessor(file);
                Future<Integer> result = executorService.submit(fileProcessor);
                results.add(result);
            }
        }

        // Process the results
        int totalLines = 0;
        
        for (Future<Integer> result : results) {
        	
            try {
                totalLines += result.get();
            } 
            catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        // Shutdown the executor service
        executorService.shutdown();

        System.out.println("Total number of lines in all files: " + totalLines);
    }
}
