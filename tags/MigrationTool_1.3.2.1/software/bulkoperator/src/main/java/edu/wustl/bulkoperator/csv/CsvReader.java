package edu.wustl.bulkoperator.csv;

/**
 * 
 * @author Vinayak Pawar (vinayak.pawar@krishagni.com)
 *
 */
public interface CsvReader {
    public String[] getColumnNames();
    
    public String getColumn(String columnName);
    
    public String getColumn(int columnIndex);
    
    public String[] getRow();
    
    public boolean next();
    
    public void close();
}
