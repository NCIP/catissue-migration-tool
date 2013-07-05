package edu.wustl.bulkoperator.csv.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import au.com.bytecode.opencsv.CSVWriter;
import edu.wustl.bulkoperator.csv.CsvException;
import edu.wustl.bulkoperator.csv.CsvWriter;

/**
 * 
 * @author Vinayak Pawar (vinayak.pawar@krishagni.com)
 *
 */
public class CsvFileWriter implements CsvWriter {
    private List<String[]> rows;
    
    private int batchSize = 1;
    
    private CSVWriter csvWriter;
    
    private int numColumns;
       
    public CsvFileWriter(CSVWriter csvWriter, String[] columnNames, int batchSize) {
        this.csvWriter = csvWriter;
        this.batchSize = batchSize;
        this.rows = new ArrayList<String[]>(batchSize);                
        this.numColumns = columnNames.length;
        rows.add(columnNames);
    }
    
    public CsvFileWriter(CSVWriter csvWriter, int numColumns, int batchSize) {
        this.csvWriter = csvWriter;
        this.batchSize = batchSize;
        this.rows = new ArrayList<String[]>(batchSize);
        this.numColumns = numColumns;
    }
    
    public static CsvFileWriter createCsvFileWriter(String outFile, String[] columnNames, int batchSize) {
        try {
            CSVWriter csvWriter = new CSVWriter(new FileWriter(outFile));
            return new CsvFileWriter(csvWriter, columnNames, batchSize);
        } catch (IOException e) {
            throw new CsvException("Error creating CSVWriter", e);
        }         
    }
    
    public static CsvFileWriter createCsvFileWriter(String outFile, int numColumns, int batchSize) {
        try {
            CSVWriter csvWriter = new CSVWriter(new FileWriter(outFile));
            return new CsvFileWriter(csvWriter, numColumns, batchSize);
        } catch (IOException e) {
            throw new CsvException("Error creating CSVWriter", e);
        }
    }
    
    public void write(List<String> colVals) {
    	rows.add(colVals.toArray(new String[0]));
    	if (rows.size() >= batchSize) {
    		flush();
    	}
    }

    public void flush() {
        try {
            csvWriter.writeAll(rows);
            csvWriter.flush();
            rows.clear();
        } catch (IOException e) {
            throw new CsvException("Error writing to CSV file", e);
        }
    }
    
    public void close() {
        try {
            flush();
            csvWriter.close();
        } catch (IOException e) {
            throw new CsvException("Error closing CSVWriter", e);
        }

    }
}