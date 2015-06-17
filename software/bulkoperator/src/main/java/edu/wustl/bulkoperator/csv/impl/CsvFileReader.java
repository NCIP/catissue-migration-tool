
package edu.wustl.bulkoperator.csv.impl;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;
import edu.wustl.bulkoperator.csv.CsvException;
import edu.wustl.bulkoperator.csv.CsvReader;

/**
 * 
 * @author Vinayak Pawar (vinayak.pawar@krishagni.com)
 *
 */
public class CsvFileReader implements CsvReader {
    
	private Map<String, List<Integer>> columnNameIdxMap = new LinkedHashMap<String, List<Integer>>();
	
	private String[] headerRow;

	private String[] currentRow;
    
    private CSVReader csvReader;
    
    private boolean isFirstRowColumnNames;
    
    public CsvFileReader(CSVReader csvReader, boolean isFirstRowColumnNames) {
        this.csvReader = csvReader;
        this.isFirstRowColumnNames = isFirstRowColumnNames;
        if (isFirstRowColumnNames) {
            createColumnNameIdxMap();
        }
    }
    
    public static CsvFileReader createCsvFileReader(InputStream inputStream, boolean isFirstRowColumnNames) {
		CSVReader csvReader = new CSVReader(new InputStreamReader(inputStream));
		return new CsvFileReader(csvReader, isFirstRowColumnNames);
    }
    
    public static CsvFileReader createCsvFileReader(String csvFile, boolean isFirstRowColumnNames) {
        try {
            CSVReader csvReader = new CSVReader(new FileReader(csvFile));
            return new CsvFileReader(csvReader, isFirstRowColumnNames);
        } catch (IOException e) {
            throw new CsvException("Error creating CSV file reader", e);
        }
    }
    
    public List<String> getHeaderRow() {
    	if (!isFirstRowColumnNames) {
    		throw new CsvException("Input file does not contain header row");
    	}
    	
    	return Arrays.asList(headerRow);
    }

	@Override
	public String getValue(String columnName) {
		return getValue(columnName, 0);
	}

	@Override
	public String getValue(String columnName, int occurence) {
		if (!isFirstRowColumnNames) {
			throw new CsvException("Input CSV does not have header row");
		}
		
		if (currentRow == null) {
			throw new CsvException("Either next() is not called or end of file is reached");
		}
		
		String result = null;
		List<Integer> indexes = columnNameIdxMap.get(columnName.trim());
		if (indexes != null && occurence < indexes.size()) {
			result = currentRow[indexes.get(occurence)];
		}
		
		return result;
	}

	@Override
	public List<String> getValues(String columnName) {
		if (!isFirstRowColumnNames) {
			throw new CsvException("Input CSV does not have header row");
		}
		
		if (currentRow == null) {
			throw new CsvException("Either next() is not called or end of file is reached");
		}
		
		List<String> result = null;
		List<Integer> indexes = columnNameIdxMap.get(columnName.trim());
		if (indexes != null) {
			result = new ArrayList<String>();
			for (int i : indexes) {
				result.add(currentRow[i]);
			}
		}

		return result;
	}

	@Override
	public String getValue(int columnIndex) {
		if (currentRow == null) {
			throw new CsvException("Either next() is not called or end of file is reached");
		}
		
		if (columnIndex < 0 || columnIndex >= currentRow.length) {
			throw new CsvException("Column index out of bounds");
		}
		
		return currentRow[columnIndex];
	}

	@Override
	public List<String> getRow() {
		return (currentRow != null) ? Arrays.asList(currentRow) : null;
	}
    
    public boolean next() {
        try {
            currentRow = csvReader.readNext();
        } catch (IOException e) {
            throw new CsvException("Error reading line from CSV file", e);
        }
        
        return (currentRow != null && currentRow.length > 0); 
    }

    public void close() {
        try {
            csvReader.close();
        } catch (IOException e) {
            throw new CsvException("Error closing CSVReader", e);
        }        
    }
    
    private void createColumnNameIdxMap() {
        try {
            String[] line = csvReader.readNext();
            if (line == null || line.length == 0) {
                throw new CsvException("CSV file column names line empty");
            }
            
            for (int i = 0; i < line.length; ++i) {
                if (line[i] == null || line[i].trim().length() == 0) {
                    throw new CsvException("CSV file column names line has empty/blank column names");
                }
                
                List<Integer> indexes = columnNameIdxMap.get(line[i].trim());
                if (indexes == null) {
                	indexes = new ArrayList<Integer>();
                	columnNameIdxMap.put(line[i].trim(), indexes);
                }
                
                indexes.add(i);
            }
            
            headerRow = line;
        } catch (IOException e) {
            throw new CsvException("Error reading CSV file column names line", e);    
        }
    }
}