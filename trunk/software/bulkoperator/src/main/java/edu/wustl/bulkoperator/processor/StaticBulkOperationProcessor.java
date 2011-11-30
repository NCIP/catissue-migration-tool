
package edu.wustl.bulkoperator.processor;

import java.util.Map;

import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.util.logger.Logger;

public class StaticBulkOperationProcessor extends AbstractBulkOperationProcessor
		implements
		IBulkOperationProcessor
{
	private static final Logger logger = Logger.getCommonLogger(StaticBulkOperationProcessor.class);

	public StaticBulkOperationProcessor(BulkOperationClass bulkOperationClass,
			AppServiceInformationObject serviceInformationObject)
	{
		super(bulkOperationClass, serviceInformationObject);
	}

	@Override
	Object processObject(Map<String, String> csvData) throws BulkOperationException
	{
		return null;
	}

	public Object process(CsvReader csvReader, int csvRowNumber,SessionDataBean sessionDataBean)
			throws BulkOperationException, Exception
	{
		Object staticObject = null;
		try
		{
			AbstractBulkOperationAppService bulkOprAppService = AbstractBulkOperationAppService.getInstance(
					serviceInformationObject.getServiceImplementorClassName(), true,
					serviceInformationObject.getUserName(), null);

			if (bulkOperationClass.isUpdateOperation())
			{
				String hql = BulkOperationUtility.createHQL(bulkOperationClass, csvReader);

				staticObject = bulkOprAppService.search(hql);
				if (staticObject == null)
				{
					throw new BulkOperationException("Could not find the specified data in the database.");
					//throw new BulkOperationException("Could not find an existing record for <update_base_on_column name> in the database.");
				}
				processObject(staticObject, bulkOperationClass, csvReader, "", false, csvRowNumber);
				bulkOprAppService.update(staticObject);
			}
			else
			{
				staticObject = getEntityObject(csvReader);
				processObject(staticObject, bulkOperationClass, csvReader, "", false, csvRowNumber);
				bulkOprAppService.insert(staticObject);
			}
		}
		catch (Exception exp){
			logger.error(exp.getMessage(), exp);
			throw exp;
		}
		return staticObject;
	}
}
