/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.migrator;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationException;
import edu.wustl.migrator.util.UnMigratedObject;



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
	
	public void handleFailedMigrationObject(Object failedObject,MigrationClass migration,String message,Throwable throwable) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
	{
		UnMigratedObject error = new UnMigratedObject();
		error.setClassName(migration.getClassName());
		error.setSandBoxId((Long)failedObject.getClass().getMethod("getId", null).invoke(failedObject, null));
		error.setMessage(throwable.getMessage());
		String stackTrace = null;

        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            pw.close();
            sw.close();
            stackTrace = sw.getBuffer().toString();
        } catch(Exception ex) {
        	ex.printStackTrace();
        }
        if(stackTrace.length()<4000)
        {
        	error.setStackTrace(stackTrace);
        }else{
		error.setStackTrace(stackTrace.substring(0, 3999));
        }
		SandBoxDao.saveObject(error);
	}
	
	public void handleSuccessfullyMigratedObject(Object mainObject, MigrationClass mainMigrationClass, ObjectIdentifierMap objectIdentifierMap) throws MigrationException
	{
		//inserts the map entry for the main object
		SandBoxDao.insertMapEntries(objectIdentifierMap);
		Collection<MigrationClass> containmentCollection = mainMigrationClass
				.getContainmentAssociationCollection();
		processContainmentObjectIdentifierMap(mainObject, mainMigrationClass, objectIdentifierMap,
				containmentCollection);
		String unMigratedObjectFlag = Migrator.unMigratedObjectFlag;
		if(unMigratedObjectFlag != null && unMigratedObjectFlag != "")
		{
			String whereColumn[] = new String[2];
			whereColumn[0] = new String("className");
			whereColumn[1] = new String("sandBoxId");
			String whereValue[] = new String[2];
			whereValue[0] = new String("'"+mainMigrationClass.getClassName()+"'");
			whereValue[1] = new String(objectIdentifierMap.getOldId().toString());
			List list = SandBoxDao.retrieve(UnMigratedObject.class.getName(), whereColumn, whereValue, null);
			if(list != null && !list.isEmpty())
			{
				SandBoxDao.delete(list.get(0));
			}
		}
	}

	/**
	 * @param mainObject
	 * @param mainMigrationClass
	 * @param objectIdentifierMap
	 * @param containmentCollection
	 * @throws MigrationException
	 */
	private void processContainmentObjectIdentifierMap(Object mainObject,
			MigrationClass mainMigrationClass, ObjectIdentifierMap objectIdentifierMap,
			Collection<MigrationClass> containmentCollection) throws MigrationException
	{
		if (containmentCollection != null)
		{
			Map<String, LinkedHashSet<ObjectIdentifierMap>> containmentObjectIdentifierMap = objectIdentifierMap
					.getContainmentObjectIdentifierMap();
			Iterator<MigrationClass> containmentMigrationClassIter = containmentCollection
					.iterator();
			while (containmentMigrationClassIter.hasNext())
			{
				MigrationClass containmentMigrationClassObj = containmentMigrationClassIter.next();
				String containmentRollName = containmentMigrationClassObj.getRoleName();
				String containmentClassName = containmentMigrationClassObj.getClassName();
				if (containmentObjectIdentifierMap.containsKey(containmentRollName))
				{
					LinkedHashSet<ObjectIdentifierMap> containmentObjectIdentifierSet = (LinkedHashSet<ObjectIdentifierMap>) containmentObjectIdentifierMap
							.get(containmentRollName);

					Object containmentObj = mainMigrationClass.invokeGetterMethod(
							containmentRollName, null, mainObject, null);
					if (containmentObj != null)
					{
						if (containmentObj instanceof Collection)
						{

							List sortedList = new ArrayList((Collection) containmentObj);
							Collections.sort(sortedList, new SortObject());
							Collection containmentObjectCollection = new LinkedHashSet(sortedList);
							if (containmentObjectIdentifierSet.size() == containmentObjectCollection
									.size())
							{
								Iterator containmentObjIter = containmentObjectCollection
										.iterator();
								Iterator<ObjectIdentifierMap> containmentObjectIdentifierSetIter = containmentObjectIdentifierSet
										.iterator();
								while (containmentObjIter.hasNext())
								{
									ObjectIdentifierMap objectIdentifier = containmentObjectIdentifierSetIter
											.next();
									Object containment = containmentObjIter.next();
									Long newId = containmentMigrationClassObj
											.invokeGetIdMethod(containment);
									objectIdentifier.setNewId(newId);
									SandBoxDao.insertMapEntries(objectIdentifier);
									Collection<MigrationClass> containmentInContainment =  containmentMigrationClassObj.getContainmentAssociationCollection();
									processContainmentObjectIdentifierMap(containment, containmentMigrationClassObj, objectIdentifier, containmentInContainment);
								
								}
							}
						}
						else
						{
							ObjectIdentifierMap objectIdentifier = containmentObjectIdentifierSet
									.iterator().next();
							Long newId = containmentMigrationClassObj
									.invokeGetIdMethod(containmentObj);
							objectIdentifier.setNewId(newId);
							SandBoxDao.insertMapEntries(objectIdentifier);
							Collection<MigrationClass> containmentInContainment =  containmentMigrationClassObj.getContainmentAssociationCollection();
							processContainmentObjectIdentifierMap(containmentObj, containmentMigrationClassObj, objectIdentifier, containmentInContainment);
						}
						
					}
				}
			}
		}
	}
	
}
