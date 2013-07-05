package edu.wustl.bulkoperator.csv;

import java.util.List;

/**
 * 
 * @author Vinayak Pawar (vinayak.pawar@krishagni.com)
 *
 */
public interface CsvWriter {
	public void write(List<String> columnValues);

	public void flush();
    
    public void close();
}
