
package edu.wustl.bulkoperator.processor;


import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.csv.impl.CsvFileReader;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.beans.SessionDataBean;

public interface IDynamicBulkOperationProcessor
{

	Object process(CsvReader csvReader, int csvRowCounter,
			SessionDataBean sessionDataBean) throws BulkOperationException,
			Exception;
}
