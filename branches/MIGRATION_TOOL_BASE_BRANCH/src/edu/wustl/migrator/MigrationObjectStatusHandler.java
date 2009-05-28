package edu.wustl.migrator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;

import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationException;



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
		throwable.printStackTrace();
	}
	
	public void handleSuccessfullyMigratedObject(ObjectIdentifierMap idMap, MigrationClass migration)
	{/*
		Object oldObj = idMap.getOldObj();
		Object newObj = idMap.getNewObj();
		Long mainObjId = null;
		Long mainObjNewId = null;
		String mainObjClassName = oldObj.getClass().getName();
		try
		{
			Object id = migration.invokeGetterMethod("id", null, oldObj, null);
			if (id != null)
			{
				mainObjId = Long.valueOf(id.toString());
			}
			
			Object newId = migration.invokeGetterMethod("id", null, newObj, null);
			if (newId != null)
			{
				mainObjNewId = Long.valueOf(newId.toString());
			}
			SandBoxDao.insertMapEntries(mainObjClassName, mainObjId, mainObjNewId);
			Collection<MigrationClass> containmentCollection = migration.getContainmentAssociationCollection();
			if(containmentCollection != null)
			{
				Iterator<MigrationClass> containmentItr = containmentCollection.iterator();
				if(containmentItr.hasNext())
				{
					MigrationClass containment = containmentItr.next();
					String containmentObjName = containment.getClassName();
					Long oldId = null;
					Object conId = null;
					Long newConId = null;
					Object conIdNew = null;
					Object conObjOld = migration.invokeGetterMethod(containment.getRoleName(), null, oldObj, null);
					Object conObjNew = migration.invokeGetterMethod(containment.getRoleName(), null, newObj, null);
					if(conObjOld != null && conObjNew != null)
					{
						if(conObjOld instanceof Collection)
						{
							Iterator it = ((Collection)conObjOld).iterator();
							Iterator itNew = ((Collection)conObjNew).iterator();
							while(it.hasNext())
							{
								Object obj = it.next();
								Object objNew = itNew.next();
								if(obj != null)
								{
									conId =  containment.invokeGetterMethod("id", null, obj, null);
									
									if (conId != null)
									{
										oldId = Long.valueOf(conId.toString());
									}
								}
								if(objNew != null)
								{
									conIdNew =  containment.invokeGetterMethod("id", null, objNew, null);;
									
									if (conIdNew != null)
									{
										newConId = Long.valueOf(conIdNew.toString());
									}
								}
								if(oldId != null && newConId != null)
									SandBoxDao.insertMapEntries(containmentObjName, oldId, newConId);
								
							}
						}
						else
						{
							conId =  containment.invokeGetterMethod("id", null, conObjOld, null);;
							
							if (conId != null)
							{
								oldId = Long.valueOf(conId.toString());
							}
							
							
								conIdNew =  containment.invokeGetterMethod("id", null, conObjNew, null);;
							
							if (conIdNew != null)
							{
								newConId = Long.valueOf(conIdNew.toString());
							}
							if(oldId != null && newConId != null)
								SandBoxDao.insertMapEntries(containmentObjName, oldId, newConId);
						}
						
					}
					

				}
			}
		}
		
		
		catch (MigrationException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//CaCoreMigrationAppServiceImpl.insertMapEntries(idMap);
		
	*/}
	
}
