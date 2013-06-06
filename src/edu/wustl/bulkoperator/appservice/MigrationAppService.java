/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue-migration-tool/LICENSE.txt for details.
 */

package edu.wustl.bulkoperator.appservice;

import java.lang.reflect.Constructor;

import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.ObjectIdentifierMap;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.exception.ErrorKey;

public abstract class MigrationAppService
{

	public MigrationAppService(boolean isAuthenticationRequired, String userName, String password)
			throws BulkOperationException
	{
		this.isAuthenticationRequired = isAuthenticationRequired;
		initialize(userName, password);
	}

	protected boolean isAuthenticationRequired = true;

	public boolean isAuthenticationRequired()
	{
		return isAuthenticationRequired;
	}

	public static MigrationAppService getInstance(String migrationAppClassName,boolean isAuthenticationRequired, String userName, String password)
			throws BulkOperationException
	{
		if(migrationAppClassName==null)
		{
			migrationAppClassName = BulkOperationConstants.CA_CORE_MIGRATION_APP_SERVICE;
		}
		MigrationAppService appService = null;
		try
		{
			Class migrationServiceTypeClass = Class.forName(migrationAppClassName);
			Class[] constructorParameters = new Class[3];
			constructorParameters[0] = boolean.class;
			constructorParameters[1] = String.class;
			constructorParameters[2] = String.class;
			Constructor constructor = migrationServiceTypeClass
					.getDeclaredConstructor(constructorParameters);
			appService = (MigrationAppService) constructor.newInstance(isAuthenticationRequired, userName, password);
		}
		catch (Exception exp)
		{
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.invalid.username.password");
			throw new BulkOperationException(errorKey, exp, "");
		}
		return appService;
	}

	abstract public void initialize(String userName, String password) throws BulkOperationException;

	abstract public void authenticate(String userName, String password)
			throws BulkOperationException;

	public void insert(Object obj, BulkOperationClass migration,
			ObjectIdentifierMap objectIdentifierMap) throws Exception
	{
		try
		{
			Object newObj = insertObject(obj);
		}
		catch (Exception appExp)
		{
			throw appExp;
		}
	}

	public Object search(Object obj) throws Exception
	{
		Object newObj = null;
		try
		{
			newObj = searchObject(obj);
		}
		catch (Exception appExp)
		{
			throw appExp;
		}
		return newObj;
	}

	public Object update(Object obj) throws Exception
	{
		Object newObj = null;
		try
		{
			newObj = updateObject(obj);
		}
		catch (Exception appExp)
		{
			throw new Exception(appExp.getMessage(), appExp);
		}
		return newObj;
	}

	abstract protected Object insertObject(Object obj) throws Exception;

	abstract public void deleteObject(Object obj) throws Exception;

	abstract protected Object updateObject(Object obj) throws Exception;

	abstract protected Object searchObject(Object obj) throws Exception;
}
