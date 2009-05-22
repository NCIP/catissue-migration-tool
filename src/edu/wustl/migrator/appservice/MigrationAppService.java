package edu.wustl.migrator.appservice;

import edu.wustl.migrator.MigrationObjectStatusHandler;
import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationException;


public abstract class  MigrationAppService
{
	public MigrationAppService(boolean isAuthenticationRequired,String userName,String password) throws MigrationException
	{
		this.isAuthenticationRequired=isAuthenticationRequired;
		initialize(userName, password);
	}
	
	protected boolean isAuthenticationRequired = true;
	
	public boolean isAuthenticationRequired()
	{
		return isAuthenticationRequired;
	}
	
	abstract public void initialize(String userName,String password) throws MigrationException;

	abstract public void authenticate(String userName,String password) throws MigrationException;
	
	public void insert(Object obj) throws MigrationException
	{
		try
		{
			ObjectIdentifierMap idMap = new ObjectIdentifierMap(obj.getClass().getName());
			idMap.setOldId(obj);
			Object newObj = insertObject(obj);
			idMap.setNewId(newObj);
			//insertMapEntries(idMap);
			MigrationObjectStatusHandler.getInstance().handleSuccessfullyMigratedObject(idMap);
		}	
		catch(Exception appExp)
		{
			MigrationObjectStatusHandler.getInstance().handleFailedMigrationObject(obj,appExp.getMessage(),appExp);
			appExp.printStackTrace();
		}
	}
	
	abstract protected Object insertObject(Object obj) throws MigrationException ;
	
	abstract public void deleteObject(Object obj) throws MigrationException;
	
	abstract public void updateObject(Object obj) throws MigrationException;

}
