package com.ducat.java.examples.multithreading.CsvImport;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultithreadedCSVProcessor {

    public static void main(String[] args) throws CsvValidationException {

        String csvFile = "/home/spxlpt099/eclipse-workspace/contacts.csv";
        int numberOfThreads = 5; // Adjust based on your requirement

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        try (CSVReader csvReader = new CSVReader(new FileReader(csvFile))) {
            String[] nextLine;
            while ((nextLine = csvReader.readNext()) != null) {
                CSVRowProcessor rowProcessor = new CSVRowProcessor(nextLine);
                executorService.submit(rowProcessor);
            }
        } 
        catch (IOException e) {
        } 
        finally {
            executorService.shutdown();
        }
    }
}
