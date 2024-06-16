package com.ducat.java.examples.multithreading.FileProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.Callable;


public class FileProcessor implements Callable<Integer> {
	
    private final File file;

    public FileProcessor(File file) {
    	
        this.file = file;
    }

    @Override
    public Integer call() throws Exception {
    	
        int lineCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        	
            while (reader.readLine() != null) {
                lineCount++;
            }
        } 
        catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(file.getName() + ": " + lineCount + " lines");
        return lineCount;
    }
}
