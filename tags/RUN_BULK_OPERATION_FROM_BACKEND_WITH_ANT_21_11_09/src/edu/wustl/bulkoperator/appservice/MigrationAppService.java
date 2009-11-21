package edu.wustl.bulkoperator.appservice;

import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.ObjectIdentifierMap;
import edu.wustl.bulkoperator.util.BulkOperationException;


public abstract class  MigrationAppService
{
	public MigrationAppService(boolean isAuthenticationRequired,String userName,String password) throws BulkOperationException
	{
		this.isAuthenticationRequired=isAuthenticationRequired;
		initialize(userName, password);
	}
	
	protected boolean isAuthenticationRequired = true;
	
	public boolean isAuthenticationRequired()
	{
		return isAuthenticationRequired;
	}
	
	abstract public void initialize(String userName,String password) throws BulkOperationException;

	abstract public void authenticate(String userName,String password) throws BulkOperationException;
	
	public void insert(Object obj,BulkOperationClass migration,ObjectIdentifierMap objectIdentifierMap) throws BulkOperationException
	{		
		try
		{
			//SandBoxDao.getCurrentSession().clear();
			Object newObj = insertObject(obj);
			//objectIdentifierMap.setNewId((Long)migration.invokeGetIdMethod(newObj));
			//MigrationObjectStatusHandler.getInstance().handleSuccessfullyMigratedObject(newObj, migration, objectIdentifierMap);
		}	
		catch(Exception appExp)
		{
			throw new BulkOperationException(appExp.getMessage(),appExp);					
		}		
	}
	
	public Object search(Object obj) throws BulkOperationException
	{		
		Object newObj = null;
		try
		{
			//SandBoxDao.getCurrentSession().clear();
			newObj = searchObject(obj);
			//objectIdentifierMap.setNewId((Long)migration.invokeGetIdMethod(newObj));
			//MigrationObjectStatusHandler.getInstance().handleSuccessfullyMigratedObject(newObj, migration, objectIdentifierMap);
		}	
		catch(Exception appExp)
		{
			throw new BulkOperationException(appExp.getMessage(),appExp);					
		}
		return newObj;
	}
	
	public Object update(Object obj) 
	throws BulkOperationException
	{		
		Object newObj = null;
		try
		{
			//SandBoxDao.getCurrentSession().clear();
			newObj = updateObject(obj);			
			//objectIdentifierMap.setNewId((Long)migration.invokeGetIdMethod(newObj));
			//MigrationObjectStatusHandler.getInstance().handleSuccessfullyMigratedObject(newObj, migration, objectIdentifierMap);
		}	
		catch(Exception appExp)
		{
			throw new BulkOperationException(appExp.getMessage(),appExp);					
		}
		return newObj;
	}
	
	abstract protected Object insertObject(Object obj) throws BulkOperationException;
	
	abstract public void deleteObject(Object obj) throws BulkOperationException;
	
	abstract protected Object updateObject(Object obj) throws BulkOperationException;	
	
	abstract protected Object searchObject(Object obj) throws BulkOperationException;
}
