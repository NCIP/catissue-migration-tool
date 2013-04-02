package edu.wustl.bulkoperator.csv;

/**
 * 
 * @author Vinayak Pawar (vinayak.pawar@krishagni.com)
 *
 */
public interface CsvWriter {
    public String[] getColumnNames();
    
    public void setColumnValue(String columnName, String columnValue);
    
    public void setColumnValue(int columnIdx, String columnValue);
    
    public void flush();
    
    public void nextRow();
    
    public void close();
}
