
package edu.wustl.bulkoperator.processor;

import java.util.Map;

import edu.wustl.bulkoperator.metadata.HookingInformation;
import edu.wustl.bulkoperator.util.BulkOperationException;

public interface IDynamicBulkOperationProcessor
{

	Object process(Map<String, String> csvData, int csvRowCounter,
			HookingInformation hookingObjectInformation) throws BulkOperationException,
			Exception;
}
