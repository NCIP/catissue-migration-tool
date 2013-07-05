
package edu.wustl.bulkoperator.processor;


import edu.wustl.bulkoperator.csv.CsvReader;

public interface IBulkOperationProcessor {
	Long process(CsvReader csvReader, int rowNumber);
}
