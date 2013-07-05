package edu.wustl.bulkoperator.csv;

import java.util.List;
import java.util.Set;

/**
 * 
 * @author Vinayak Pawar (vinayak.pawar@krishagni.com)
 *
 */
public interface CsvReader {
    public List<String> getHeaderRow();
    
    public String getValue(String columnName);
    
    public String getValue(String columnName, int occurence);
    
    public List<String> getValues(String columnName);
    
    public String getValue(int columnIndex);
    
    public List<String> getRow();
    
    public boolean next();
    
    public void close();
}
