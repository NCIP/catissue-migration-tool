
package edu.wustl.bulkoperator.processor;

import java.util.Map;

import edu.wustl.bulkoperator.util.BulkOperationException;

public interface IStaticBulkOperationProcessor
{
	Object process(Map<String, String> csvData, int csvRowNumber) throws BulkOperationException, Exception;
}
