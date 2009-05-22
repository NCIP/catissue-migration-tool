
package edu.wustl.migrator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import edu.wustl.catissuecore.domain.Specimen;
import edu.wustl.migrator.dao.HibernateSessionHandler;
import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.util.MigrationException;
import edu.wustl.migrator.util.MigrationUtility;

public class MigrationProcessor
{

	MigrationClass migrationClass = null;

	public MigrationClass getMigration()
	{
		return migrationClass;
	}

	public void setMigration(MigrationClass migration)
	{
		this.migrationClass = migration;
	}

	List<Long> ids = new ArrayList<Long>();

	public List<Long> getIds()
	{
		return ids;
	}

	public void setIds(List<Long> ids)
	{
		this.ids = ids;
	}

	public MigrationProcessor(MigrationClass migration)
	{
		this.migrationClass = migration;
		this.ids = ids;
	}

	/**
	 * 
	 * @return
	 */
	public void fetchObjectIdentifier()
	{
		ids.clear();
		List list = SandBoxDao.executeSQLQuery(migrationClass.getSql());
		ids.addAll(list);
	}

	/**
	 * 
	 * @param mainObjectsList
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public List<Object> fetchObjects() throws ClassNotFoundException, SecurityException,
			NoSuchMethodException, IllegalArgumentException, IllegalAccessException,
			InvocationTargetException
	{
		List<Object> listForInsertion = new ArrayList<Object>();
		try
		{
			fetchObjectIdentifier();
			String className = migrationClass.getClassName();
			String query = null;
			List<Object> objectList = null;
			if (ids == null && ids.isEmpty())
			{
				query = "from " + className;
			}
			else
			{
				String idsSet = "(";
				Iterator<Long> idItert = ids.iterator();
				int noOfIds = ids.size();
				int counter = 1;
				while (idItert.hasNext())
				{
					Long id = idItert.next();
					if (counter == noOfIds)
					{
						idsSet += id;
					}
					else
					{
						idsSet += id + ",";
					}
					counter++;
				}
				idsSet += ")";
				query = "from " + className + " where id in " + idsSet;
			}
			objectList = SandBoxDao.executeHQLQuery(query);

			if (objectList != null && !objectList.isEmpty())
			{
				Collections.sort(objectList, new SortObject());
				Iterator<Object> iterator = objectList.iterator();
				while (iterator.hasNext())
				{
					//object of the type = "main"
					Object object = iterator.next();
					Specimen s = (Specimen) object;
					if (!s.getLineage().equalsIgnoreCase("New"))
					{
						processObject(object, migrationClass);

						listForInsertion.add(object);
					}
				}
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			//dao.destroyConnection(con);
			HibernateSessionHandler.closeSession();
		}
		return listForInsertion;
	}

	/**
	 * @param className
	 * @param mainObj
	 * @throws ClassNotFoundException
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	private void processObject(Object mainObj, MigrationClass migrationClass)
			throws MigrationException
	{

		if (migrationClass.getReferenceAssociationCollection() != null
				&& !migrationClass.getReferenceAssociationCollection().isEmpty())
		{
			Collection<MigrationClass> associations = migrationClass
					.getReferenceAssociationCollection();

			processAssociations(mainObj, migrationClass, associations);

		}
		if (migrationClass.getContainmentAssociationCollection() != null
				&& !migrationClass.getContainmentAssociationCollection().isEmpty())
		{
			Collection<MigrationClass> containmentMigrationClassList = migrationClass
					.getContainmentAssociationCollection();
			processContainments(mainObj, migrationClass, containmentMigrationClassList);
		}
	}

	/**
	 * @param mainObj
	 * @param mainObjectClass
	 * @param containments
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws ClassNotFoundException 
	 * @throws InstantiationException 
	 */
	private void processContainments(Object mainObj, MigrationClass mainMigrationClass,
			Collection<MigrationClass> containmentMigrationClassList) throws MigrationException
	{

		Iterator<MigrationClass> containmentItert = containmentMigrationClassList.iterator();

		while (containmentItert.hasNext())
		{
			MigrationClass containmentMigrationClass = containmentItert.next();


			String cardinality = containmentMigrationClass.getCardinality();
			String isToSetNull = containmentMigrationClass.getIsToSetNull();
			if (cardinality != null && cardinality.equals("*") && cardinality != "")
			{
				Collection containmentObjectCollection = (Collection) mainMigrationClass.invokeGetterMethod(
						containmentMigrationClass.getRoleName(), null, mainObj, null);
				//getContainment.invoke(mainObj, null);

				//added for participant race
				Collection<Object> newContainmentObjectCollection = new HashSet<Object>();

				if (containmentObjectCollection != null && !containmentObjectCollection.isEmpty())
				{
					if (isToSetNull.equalsIgnoreCase("Yes"))
					{
						newContainmentObjectCollection = null;
					}
					else
					{
						Iterator collIter = containmentObjectCollection.iterator();
						while (collIter.hasNext())
						{
							Object containmentObject = collIter.next();
							processObject(containmentObject, containmentMigrationClass);
							containmentMigrationClass.invokeSetIdMethod(containmentObject, null);
							newContainmentObjectCollection.add(containmentObject);
						}
					}
					//added for participant race
//					String setterForContainment = getFunctionName(containmentMigrationClass
//							.getRoleName(), "set");
//					Method setContainment = mainObjectClass.getMethod(setterForContainment,
//							Collection.class);
					String roleName = containmentMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{Collection.class},mainObj, newContainmentObjectCollection);
//					setContainment.invoke(mainObj, newContainmentObjectCollection);
				}
			}
			else if (cardinality != null && cardinality.equals("1") && cardinality != "")
			{
				Object containmentObject = mainMigrationClass.invokeGetterMethod(
						containmentMigrationClass.getRoleName(), null, mainObj, null);

				if (containmentObject != null)
				{
					if (isToSetNull.equalsIgnoreCase("Yes"))
					{
						containmentObject = null;
					}
					else
					{
						processObject(containmentObject, containmentMigrationClass);
						containmentMigrationClass.invokeSetIdMethod(containmentObject, null);
					}
					String roleName = containmentMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{containmentMigrationClass.getClass()}, mainObj, containmentObject);

				}
			}
		}
	}

	/**
	 * @param mainObj
	 * @param mainObjectClass
	 * @param associationsMigrationClassList
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 */
	private void processAssociations(Object mainObj, MigrationClass mainMigrationClass,
			Collection<MigrationClass> associationsMigrationClassList) throws MigrationException
	{
		//Class<?> mainObjectClass = Class.forName(mainMigrationClass.getClassName());
		Iterator<MigrationClass> associationItert = associationsMigrationClassList.iterator();
		while (associationItert.hasNext())
		{
			// the associated object to the main object
			MigrationClass associationMigrationClass = associationItert.next();

			//the function name related to the associated object used in the main object
			String cardinality = associationMigrationClass.getCardinality();
			String isToSetNull = associationMigrationClass.getIsToSetNull();
			//Class<?> associatedClass = Class.forName(associationMigrationClass.getClassName());
			if (cardinality != null && cardinality.equals("*") && cardinality != ""
					&& mainObj != null)
			{
				Collection<Object> newAssociationCollection = new LinkedHashSet<Object>();

				Collection associationObjectCollection = (Collection) mainMigrationClass.invokeGetterMethod(
						associationMigrationClass.getRoleName(), null, mainObj, null);
				//getterForAssociation.invoke(mainObj, null);
				if (associationObjectCollection != null && !associationObjectCollection.isEmpty())
				{
					if (isToSetNull.equalsIgnoreCase("Yes"))
					{
						newAssociationCollection = null;
					}
					else
					{
						Iterator collectionIterator = associationObjectCollection.iterator();
						while (collectionIterator.hasNext())
						{
							Object associatedObject = collectionIterator.next();
							processObject(associatedObject, associationMigrationClass);
							//get id must of the persistent object
							//Method getId = persitentObj.getClass().getMethod("getId", null);
							//Object id = getId.invoke(persitentObj, null);
							Long sandBoxId = associationMigrationClass.invokeGetIdMethod(associatedObject);
							Long productionId = SandBoxDao.getProductionId(sandBoxId, associationMigrationClass.getClassName());
							
							// setid must be of the new object class
							Object newAssociatedObject = associationMigrationClass.getNewInstance(); 
							
							//associatedClass.newInstance();
							associationMigrationClass.invokeSetIdMethod(newAssociatedObject, productionId);
							
							// to remove this if loop
							if (productionId == null)
							{
								productionId = Long.valueOf(sandBoxId.toString());
							}
							// end of to remove
							newAssociationCollection.add(newAssociatedObject);
						}

					}
					String roleName = associationMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{Collection.class},mainObj, newAssociationCollection);
				}

			}
			else if (cardinality != null && cardinality.equals("1") && cardinality != "")
			{
				Object associatedObject = mainMigrationClass.invokeGetterMethod(
						associationMigrationClass.getRoleName(), null, mainObj, null);
				processObject(associatedObject, associationMigrationClass);
					//getterForAssociation.invoke(mainObj, null);
				Object newAssociatedObject = null;

				if (associatedObject != null)
				{
					if (!"Yes".equalsIgnoreCase(isToSetNull))
					{				
						//setting the production id to the same object
						Long sandBoxId = associationMigrationClass.invokeGetIdMethod(associatedObject);
						Long productionId = SandBoxDao.getProductionId(sandBoxId, associationMigrationClass.getClassName());
						newAssociatedObject = associationMigrationClass.getNewInstance();
						//setting the production id to the new object
						associationMigrationClass.invokeSetIdMethod(newAssociatedObject, productionId);
						
						// to remove this if loop
						if (productionId == null)
						{
							productionId = Long.valueOf(sandBoxId.toString());
						}
						// end of to remove

						String roleName = associationMigrationClass.getRoleName();
						mainMigrationClass.invokeSetterMethod(roleName, new Class[]{associationMigrationClass.getClass()},mainObj, newAssociatedObject);
					}
				}
			}
		}
	}
}