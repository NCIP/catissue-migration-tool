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

import java.util.ArrayList;
import java.util.Map;

import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.dao.DAO;
import edu.wustl.dao.daofactory.DAOConfigFactory;
import edu.wustl.dao.daofactory.IDAOFactory;

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
		DAO dao=null;
		final IDAOFactory daofactory = DAOConfigFactory.getInstance().getDAOFactory(
				CommonServiceLocator.getInstance().getAppName());
		dao = daofactory.getDAO();
		dao.openSession(null);
		try
		{
			AbstractBulkOperationAppService bulkOprAppService = AbstractBulkOperationAppService.getInstance(
					serviceInformationObject.getServiceImplementorClassName(), true,
					serviceInformationObject.getUserName(), null);

			if (bulkOperationClass.isUpdateOperation())
			{
				String hql = BulkOperationUtility.createHQL(bulkOperationClass, csvReader);
				ArrayList<Object> objects=(ArrayList<Object>)dao.executeQuery(hql);
				staticObject =objects.get(0);
				if (staticObject == null)
				{
					throw new BulkOperationException("Could not find the specified data in the database.");
				}
				else
				{
					
					processObject(staticObject, bulkOperationClass, csvReader, "", false, csvRowNumber);
					try
					{
						dao.closeSession();
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
				staticObject = getEntityObject(csvReader);
				processObject(staticObject, bulkOperationClass, csvReader, "", false, csvRowNumber);
				bulkOprAppService.insert(staticObject);
			}
		}
		catch (Exception exp){
			logger.error(exp.getMessage(), exp);
			throw exp;
		}
		finally
		{
			if(dao!=null)
			{
				dao.closeSession();
			}
		}
		return staticObject;
	}
}
