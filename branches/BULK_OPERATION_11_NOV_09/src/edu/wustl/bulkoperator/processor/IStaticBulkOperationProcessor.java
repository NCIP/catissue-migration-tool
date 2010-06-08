
package edu.wustl.bulkoperator.processor;

import java.util.Map;

import edu.wustl.bulkoperator.metadata.HookingInformation;
import edu.wustl.bulkoperator.util.BulkOperationException;

public interface IStaticBulkOperationProcessor
{
	Object process(Map<String, String> csvData, int csvRowNumber, HookingInformation hookingInformation) throws BulkOperationException, Exception;
}
