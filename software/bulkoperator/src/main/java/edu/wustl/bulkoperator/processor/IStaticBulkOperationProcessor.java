
package edu.wustl.bulkoperator.processor;


import edu.wustl.bulkoperator.csv.impl.CsvFileReader;
import edu.wustl.bulkoperator.util.BulkOperationException;

public interface IStaticBulkOperationProcessor
{
	Object process(CsvFileReader csvFileReader, int csvRowNumber) throws BulkOperationException, Exception;
}
