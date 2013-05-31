/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.bulkoperator.controller;

import java.util.ArrayList;
import java.util.List;

import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.processor.DynCategoryBulkOperationProcessor;
import edu.wustl.bulkoperator.processor.DynEntityBulkOperationProcessor;
import edu.wustl.bulkoperator.processor.IDynamicBulkOperationProcessor;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.logger.Logger;

public class BulkOperationControllerFactory
{
	private static final Logger logger = Logger.getCommonLogger(BulkOperationControllerFactory.class);
	private static BulkOperationControllerFactory factory = null;

	private BulkOperationControllerFactory()
	{}

	public static BulkOperationControllerFactory getInstance()
	{
		if (factory == null)
		{
			factory = new BulkOperationControllerFactory();
		}
		return factory;
	}

	public List<IDynamicBulkOperationProcessor> getAllDynamicBulkOperationProcessor(
			BulkOperationClass bulkOperationClass,
			AppServiceInformationObject serviceInformationObject) throws BulkOperationException
	{
		List<IDynamicBulkOperationProcessor> dynamicBulkOperationClassList = new ArrayList<IDynamicBulkOperationProcessor>();
		try
		{	
			if (bulkOperationClass.checkForDynEntityAssociationCollectionTag(bulkOperationClass))
			{
				BulkOperationClass dynExtEntityBOClass = BulkOperationUtility.checkForDEObject(bulkOperationClass);
				if(dynExtEntityBOClass == null)
				{
					logger.error("Error while creating DEAssocationClass instance", null);
					ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.creating.de.bulkoperation.class");
					throw new BulkOperationException(errorkey, null, "");
				}
				else
				{
					dynamicBulkOperationClassList.add(new DynEntityBulkOperationProcessor(
						dynExtEntityBOClass, serviceInformationObject));
					logger.debug("In getAllDynamicBulkOperationProcessor method. DE Object list size: " + 
						dynamicBulkOperationClassList.size());
				}
			}
			if (bulkOperationClass.checkForDynExtCategoryAssociationCollectionTag(bulkOperationClass))
			{
				BulkOperationClass categorybulkOperationClass = BulkOperationUtility.checkForCategoryObject(bulkOperationClass);
				if(categorybulkOperationClass == null)
				{
					logger.error("Error while creating DEAssocationClass instance", null);
					ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.creating.de.bulkoperation.class");
					throw new BulkOperationException(errorkey, null, "");
				}
				else
				{
					dynamicBulkOperationClassList.add(new DynCategoryBulkOperationProcessor(
							categorybulkOperationClass, serviceInformationObject));
					logger.debug("In getAllDynamicBulkOperationProcessor method. DE Object list size: " + 
						dynamicBulkOperationClassList.size());
				}
			}
		}
		catch (BulkOperationException bulkOprExp)
		{
			throw bulkOprExp;
		}
		return dynamicBulkOperationClassList;
	}
}