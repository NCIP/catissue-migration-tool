
package edu.wustl.bulkoperator.processor;


import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.csv.impl.CsvFileReader;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.beans.SessionDataBean;

public interface IBulkOperationProcessor
{
	Object process(CsvReader csvReader, int csvRowNumber,SessionDataBean sessionDataBean) throws BulkOperationException, Exception;
}
