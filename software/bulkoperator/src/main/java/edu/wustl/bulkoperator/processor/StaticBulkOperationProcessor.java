
package edu.wustl.bulkoperator.processor;

import java.util.Map;

import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.csv.impl.CsvFileReader;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.util.logger.Logger;

public class StaticBulkOperationProcessor extends AbstractBulkOperationProcessor
		implements
			IStaticBulkOperationProcessor
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

	public Object process(CsvFileReader csvFileReader, int csvRowNumber)
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
				String hql = BulkOperationUtility.createHQL(bulkOperationClass, csvFileReader);

				staticObject = bulkOprAppService.search(hql);
				if (staticObject == null)
				{
					throw new BulkOperationException("Could not find the specified data in the database.");
				}
				else
				{
					processObject(staticObject, bulkOperationClass, csvFileReader, "", false, csvRowNumber);
					try
					{
						bulkOprAppService.update(staticObject);
					}
					catch (BulkOperationException bulkOprExp)
					{
						throw bulkOprExp;
 					}
				}
			}
			else
			{
				staticObject = getEntityObject(csvFileReader);
				processObject(staticObject, bulkOperationClass, csvFileReader, "", false, csvRowNumber);
				bulkOprAppService.insert(staticObject);
			}
		}
		catch (BulkOperationException bulkOprExp)
		{
			logger.error(bulkOprExp.getMessage(), bulkOprExp);
			throw bulkOprExp;
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			throw exp;
		}
		return staticObject;
	}
}