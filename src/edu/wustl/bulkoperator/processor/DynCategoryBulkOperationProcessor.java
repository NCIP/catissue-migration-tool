
package edu.wustl.bulkoperator.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.wustl.bulkoperator.HookingObjectInformation;
import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.metadata.Attribute;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.Validator;
import edu.wustl.common.util.logger.Logger;

public class DynCategoryBulkOperationProcessor extends AbstractBulkOperationProcessor
		implements
			IDynamicBulkOperationProcessor
{

	private static final Logger logger = Logger
			.getCommonLogger(DynCategoryBulkOperationProcessor.class);

	public DynCategoryBulkOperationProcessor(BulkOperationClass DEbulkOperationClass,
			AppServiceInformationObject serviceInformationObject)
	{
		super(DEbulkOperationClass, serviceInformationObject);
	}

	public Object process(Map<String, String> csvData, int csvRowCounter,
			HookingObjectInformation hookingObjectInformation) throws BulkOperationException,
			Exception
	{
		Object dynExtObject = new HashMap<Long, Object>();

		try
		{
			AbstractBulkOperationAppService bulkOprAppService = AbstractBulkOperationAppService
					.getInstance(serviceInformationObject.getServiceImplementorClassName(), true,
							serviceInformationObject.getUserName(), null);
			processObject(dynExtObject, bulkOperationClass, csvData, "", false, csvRowCounter);

			Map<Long, Object> categoryDataValueMap = (Map<Long, Object>) dynExtObject;

			Long recordId = bulkOprAppService.insertData(bulkOperationClass.getId(),
					categoryDataValueMap);
			hookingObjectInformation.setDynamicExtensionObjectId(recordId);
			hookingObjectInformation.setContainerId(Long.valueOf(bulkOperationClass
					.getContainerId()));
			bulkOprAppService.hookStaticDEObject(hookingObjectInformation);
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
		return dynExtObject;
	}

	@Override
	Object processObject(Map<String, String> csvData) throws BulkOperationException
	{
		// TODO Auto-generated method stub
		return null;
	}

	protected void setValueToObject(Object mainObj, BulkOperationClass mainMigrationClass,
			Map<String, String> csvData, String columnSuffix, boolean validate,
			Attribute attribute, Class dataTypeClass) throws BulkOperationException
	{
		String csvDataValue = csvData.get(attribute.getCsvColumnName() + columnSuffix);
		Object attributeValue = attribute.getValueOfDataType(csvDataValue, validate);
		Map<Long, Object> categoryDataValueMap = (Map<Long, Object>) mainObj;
		categoryDataValueMap.put(attribute.getId(), attributeValue);
	}

	/**
	 * 
	 * @param mainObj
	 * @param mainMigrationClass
	 * @param csvData 
	 * @param columnSuffix
	 * @param validate
	 * @throws BulkOperationException
	 */
	protected void processContainments(Object mainObj, BulkOperationClass mainMigrationClass,
			Map<String, String> csvData, String columnSuffix, boolean validate, int csvRowNumber)
			throws BulkOperationException
	{
		try
		{
			Map<Long, Object> categoryDataValueMap = (Map<Long, Object>) mainObj;

			Iterator<BulkOperationClass> containmentItert = mainMigrationClass
					.getContainmentAssociationCollection().iterator();
			while (containmentItert.hasNext())
			{
				BulkOperationClass containmentMigrationClass = containmentItert.next();
				String cardinality = containmentMigrationClass.getCardinality();
				if (cardinality != null && cardinality.equals("*"))
				{
					List<Map<Long, Object>> list = new ArrayList<Map<Long, Object>>();

					int maxNoOfRecords = containmentMigrationClass.getMaxNoOfRecords().intValue();
					for (int i = 1; i <= maxNoOfRecords; i++)
					{
						List<String> attributeList = BulkOperationUtility.getAttributeList(
								containmentMigrationClass, columnSuffix + "#" + i);
						if (BulkOperationUtility.checkIfAtLeastOneColumnHasAValue(csvRowNumber,
								attributeList, csvData)
								|| validate)
						{
							Object obj = new HashMap<Long, Object>();
							processObject(obj, containmentMigrationClass, csvData, columnSuffix
									+ "#" + i, validate, csvRowNumber);
							list.add((Map<Long, Object>) obj);

						}
						categoryDataValueMap.put(containmentMigrationClass.getId(), list);
					}

				}
				else if (cardinality != null && cardinality.equals("1"))
				{
					List<Map<Long, Object>> list = new ArrayList<Map<Long, Object>>();
					Object obj = new HashMap<Long, Object>();
					processObject(obj, containmentMigrationClass, csvData, "", false, csvRowNumber);

					list.add((Map<Long, Object>) obj);
					categoryDataValueMap.put(containmentMigrationClass.getId(), list);
				}
			}
		}
		catch (BulkOperationException bulkExp)
		{
			logger.error(bulkExp.getMessage(), bulkExp);
			throw new BulkOperationException(bulkExp.getErrorKey(), bulkExp, bulkExp.getMsgValues());
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
	}

}
