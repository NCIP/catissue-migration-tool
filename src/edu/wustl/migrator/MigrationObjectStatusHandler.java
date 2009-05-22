package edu.wustl.migrator;

import edu.wustl.migrator.metadata.ObjectIdentifierMap;


public class MigrationObjectStatusHandler
{
	static MigrationObjectStatusHandler failurehandler=null;
	private MigrationObjectStatusHandler()
	{
		
	}
	
	public static MigrationObjectStatusHandler getInstance()
	{
		if(failurehandler==null)
		{
			failurehandler = new MigrationObjectStatusHandler();
		}
		return failurehandler;
	}
	
	public void handleFailedMigrationObject(Object failedObject,String message,Throwable throwable)
	{
		
	}
	
	public void handleSuccessfullyMigratedObject(ObjectIdentifierMap idMap)
	{
		
	}
	
}
