
package edu.wustl.bulkoperator.processor;

import java.util.Map;

import edu.wustl.bulkoperator.HookingObjectInformation;
import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.util.logger.Logger;

public class DynEntityBulkOperationProcessor extends AbstractBulkOperationProcessor
		implements
			IDynamicBulkOperationProcessor
{
	private static final Logger logger = Logger.getCommonLogger(DynEntityBulkOperationProcessor.class);
	
	public DynEntityBulkOperationProcessor()
	{

	}
	
	public DynEntityBulkOperationProcessor(BulkOperationClass DEbulkOperationClass,
			AppServiceInformationObject serviceInformationObject)
	{
		super(DEbulkOperationClass, serviceInformationObject);
	}

	public Object process(Map<String, String> csvData,
			int csvRowCounter, HookingObjectInformation hookingObjectInformation)
			throws BulkOperationException, Exception
	{
		Object dynExtObject = null;
		try
		{
			AbstractBulkOperationAppService bulkOprAppService = AbstractBulkOperationAppService.getInstance(
					serviceInformationObject.getServiceImplementorClassName(), true,
					serviceInformationObject.getUserName(), null);
			
			dynExtObject = bulkOperationClass.getClassDiscriminator(csvData, "");
			if (dynExtObject == null)
			{
				dynExtObject = bulkOperationClass.getNewInstance();
			}
			processObject(dynExtObject, bulkOperationClass, csvData, "", false, csvRowCounter);
			bulkOprAppService.insertDEObject(dynExtObject, hookingObjectInformation.getStaticObject());
			
			hookingObjectInformation.setDynamicExtensionObjectId(
					getBulkOperationClass().invokeGetIdMethod(dynExtObject));
			hookingObjectInformation.setContainerId(Long.valueOf(bulkOperationClass.getContainerId()));
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
}