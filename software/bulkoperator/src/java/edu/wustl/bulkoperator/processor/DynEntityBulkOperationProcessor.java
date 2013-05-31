/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.bulkoperator.processor;

import java.util.List;
import java.util.Map;

import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.HookingInformation;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.util.logger.Logger;

public class DynEntityBulkOperationProcessor extends AbstractBulkOperationProcessor
		implements
			IDynamicBulkOperationProcessor
{
	private static final Logger logger = Logger.getCommonLogger(DynEntityBulkOperationProcessor.class);

	public DynEntityBulkOperationProcessor(BulkOperationClass dynExtEntityBOClass,
			AppServiceInformationObject serviceInformationObject)
	{
		super(dynExtEntityBOClass, serviceInformationObject);
	}

	public Object process(Map<String, String> csvData,
			int csvRowCounter, HookingInformation hookingObjectInformation)
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

			HookingInformation hookingInformationFromTag=((List<HookingInformation>)bulkOperationClass.getHookingInformation()).get(0);
			getinformationForHookingData(csvData,hookingInformationFromTag);

			
			hookingObjectInformation.setDynamicExtensionObjectId(
					getBulkOperationClass().invokeGetIdMethod(dynExtObject));
			hookingObjectInformation.setRootContainerId(hookingInformationFromTag.getRootContainerId());
			bulkOprAppService.insertDEObject(dynExtObject, hookingObjectInformation.getStaticObject());
			
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