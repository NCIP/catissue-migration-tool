
package edu.wustl.bulkoperator.processor;


import edu.wustl.bulkoperator.csv.impl.CsvFileReader;
import edu.wustl.bulkoperator.metadata.HookingInformation;
import edu.wustl.bulkoperator.util.BulkOperationException;

public interface IDynamicBulkOperationProcessor
{

	Object process(CsvFileReader csvFileReader, int csvRowCounter,
			HookingInformation hookingObjectInformation) throws BulkOperationException,
			Exception;
}
