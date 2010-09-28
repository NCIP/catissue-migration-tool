
package edu.wustl.bulkoperator.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.metadata.Attribute;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.HookingInformation;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.logger.Logger;

public class DynCategoryBulkOperationProcessor extends AbstractBulkOperationProcessor
		implements
			IDynamicBulkOperationProcessor
{

	private static final Logger logger = Logger
			.getCommonLogger(DynCategoryBulkOperationProcessor.class);

	public DynCategoryBulkOperationProcessor(BulkOperationClass dynExtCategoryBOClass,
			AppServiceInformationObject serviceInformationObject)
	{
		super(dynExtCategoryBOClass, serviceInformationObject);
	}

	public Object process(Map<String, String> csvData, int csvRowCounter,
			HookingInformation hookingObjectInformation) throws BulkOperationException,
			Exception
	{

		HashMap<String,Object> dynExtObject=new HashMap<String, Object>();
		Long recordId=null;
		try
		{
			AbstractBulkOperationAppService bulkOprAppService = AbstractBulkOperationAppService
					.getInstance(serviceInformationObject.getServiceImplementorClassName(), true,
							serviceInformationObject.getUserName(), null);
			processObject(dynExtObject, bulkOperationClass, csvData, "", false, csvRowCounter);
			HookingInformation hookingInformationFromTag=((List<HookingInformation>)bulkOperationClass.getHookingInformation()).get(0);
			getinformationForHookingData(csvData,hookingInformationFromTag);
			recordId = bulkOprAppService.insertData(bulkOperationClass.getClassName(),
					dynExtObject);
			hookingInformationFromTag.setCategoryName(bulkOperationClass.getClassName());
			hookingInformationFromTag.setDynamicExtensionObjectId(recordId);
			hookingInformationFromTag.setSessionDataBean(hookingObjectInformation.getSessionDataBean());
			bulkOprAppService.hookStaticDEObject(hookingInformationFromTag);
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
		return recordId;
	}

	@Override
	Object processObject(Map<String, String> csvData) throws BulkOperationException
	{
		return null;
	}

	protected void setValueToObject(Object mainObj, BulkOperationClass mainMigrationClass,
			Map<String, String> csvData, String columnSuffix, boolean validate,
			Attribute attribute, Class dataTypeClass) throws BulkOperationException
	{
		String csvDataValue = csvData.get(attribute.getCsvColumnName() + columnSuffix);
		Object attributeValue = attribute.getValueOfDataType(csvDataValue, validate);
		Map<String, Object> categoryDataValueMap = (Map<String, Object>) mainObj;
		categoryDataValueMap.put(attribute.getName(), attributeValue);
	}

	/**
	 *
	 * @param mainObj
	 * @param bulkOperationClass
	 * @param csvData
	 * @param columnSuffix
	 * @param validate
	 * @throws BulkOperationException
	 */
	protected void processContainments(Object mainObj, BulkOperationClass bulkOperationClass,
			Map<String, String> csvData, String columnSuffix, boolean validate, int csvRowNumber)
			throws BulkOperationException
	{
		try
		{
			Map<String, Object> categoryDataValueMap = (Map<String, Object>) mainObj;

			Iterator<BulkOperationClass> containmentItert = bulkOperationClass
					.getContainmentAssociationCollection().iterator();
			while (containmentItert.hasNext())
			{
				BulkOperationClass containmentObjectCollection = containmentItert.next();
				String cardinality = containmentObjectCollection.getCardinality();
				if (cardinality != null && cardinality.equals("*"))
				{
					List<Map<Long, Object>> list = new ArrayList<Map<Long, Object>>();

					int maxNoOfRecords = containmentObjectCollection.getMaxNoOfRecords().intValue();
					for (int i = 1; i <= maxNoOfRecords; i++)
					{
						List<String> attributeList = BulkOperationUtility.getAttributeList(
								containmentObjectCollection, columnSuffix + "#" + i);
						if (BulkOperationUtility.checkIfAtLeastOneColumnHasAValue(csvRowNumber,
								attributeList, csvData) || validate)
						{
							Object obj = new HashMap<Long, Object>();
							processObject(obj, containmentObjectCollection, csvData, columnSuffix
									+ "#" + i, validate, csvRowNumber);
							list.add((Map<Long, Object>) obj);
						}
						categoryDataValueMap.put(containmentObjectCollection.getClassName(), list);
					}
				}
				else if (cardinality != null && cardinality.equals("1"))
				{
					List<Map<Long, Object>> list = new ArrayList<Map<Long, Object>>();
					Object obj = new HashMap<Long, Object>();
					processObject(obj, containmentObjectCollection, csvData, columnSuffix
							+ "#" + 1, false, csvRowNumber);

					list.add((Map<Long, Object>) obj);
					categoryDataValueMap.put(containmentObjectCollection.getClassName(), list);
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