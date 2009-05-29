package edu.wustl.migrator.appservice;

import edu.wustl.migrator.MigrationObjectStatusHandler;
import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.MigrationClass;
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
	
	public void insert(Object obj,MigrationClass migration,ObjectIdentifierMap objectIdentifierMap) throws MigrationException
	{
		
		try
		{
			ObjectIdentifierMap  objectIdentifier = new ObjectIdentifierMap(migration.getClassName());
			objectIdentifier.setOldId(migration.invokeGetIdMethod(obj));
			Object newObj = insertObject(obj);
			objectIdentifier.setNewId(migration.invokeGetIdMethod(newObj));
			SandBoxDao.insertMapEntries(objectIdentifier);
			//processObjectIdentifierMap();
			//idMap.setNewObj(newObj);
			//idMap.setNewId(newObj);
			//insertMapEntries(idMap);
			MigrationObjectStatusHandler.getInstance().handleSuccessfullyMigratedObject(newObj, migration, objectIdentifierMap);
		}	
		catch(Exception appExp)
		{
			MigrationObjectStatusHandler.getInstance().handleFailedMigrationObject(obj,appExp.getMessage(),appExp);
			//appExp.printStackTrace();
		}
		
	}
	
	abstract protected Object insertObject(Object obj) throws MigrationException ;
	
	abstract public void deleteObject(Object obj) throws MigrationException;
	
	abstract public void updateObject(Object obj) throws MigrationException;

}
