package com.ducat.java.examples.multithreading.CsvImport;

public class CSVRowProcessor implements Runnable {
    
    private final String[] row;

    public CSVRowProcessor(String[] row) {
        this.row = row;
    }

    @Override
    public void run() {
        // Process the row
        System.out.println(Thread.currentThread().getName() + " - Processing row: " + String.join(", ", row));
        // Here you can add more processing logic
    }
}
