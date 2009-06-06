
package edu.wustl.migrator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.wustl.migrator.appservice.MigrationAppService;
import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.Attribute;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationException;

public class MigrationProcessor
{

	MigrationAppService migrationAppService;
	MigrationClass migrationClass = null;
	ObjectIdentifierMap objectMap;	

	public MigrationClass getMigration()
	{
		return migrationClass;
	}

	public void setMigration(MigrationClass migration, MigrationAppService migrationAppService)
	{
		this.migrationClass = migration;
		this.migrationAppService=migrationAppService;
	}

	List<Long> ids = new LinkedList<Long>();

	public List<Long> getIds()
	{
		return ids;
	}

	public void setIds(List<Long> ids)
	{
		this.ids = ids;
	}

	public MigrationProcessor(MigrationClass migration, MigrationAppService migrationAppService)
	{
		this.migrationClass = migration;
		this.migrationAppService = migrationAppService;
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
	public void fetchObjects() throws ClassNotFoundException, SecurityException,
			NoSuchMethodException, IllegalArgumentException, IllegalAccessException,
			InvocationTargetException
	{
		Map<Object, Object> sandBoxObjs = new HashMap<Object, Object>();
		
		//List<Object> listForInsertion = new ArrayList<Object>();
		try
		{
			if(SandBoxDao.getCurrentSession() == null)
			{
				SandBoxDao.getNewSession();
			}
			System.out.println("sadas");
			fetchObjectIdentifier();
			String className = migrationClass.getClassName();
			String query = null;
			List<Object> objectList = null;
			for(int i=0;i<ids.size();i++)
			{
				query = "from " + migrationClass.getClassName() + " where id = " + ids.get(i);
				objectList = SandBoxDao.executeHQLQuery(query.toString());
				if (objectList != null && !objectList.isEmpty())
				{
					Object object = objectList.get(0);
					objectMap = new ObjectIdentifierMap(migrationClass.getClassName());	
					objectMap.setOldId(migrationClass.invokeGetIdMethod(object));
					
					processObject(object, migrationClass,objectMap);
					System.out.println(object);
					migrationAppService.insert(object, migrationClass,objectMap);
				
					//listForInsertion.add(object);
				}

			}
			
			

			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			SandBoxDao.closeSession();
		}
	}

	/**
	 * @param className
	 * @param mainObj
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws ClassNotFoundException
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	private void processObject(Object mainObj, MigrationClass migrationClass,ObjectIdentifierMap objectMap)
			throws MigrationException, InstantiationException, IllegalAccessException
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
			processContainments(mainObj, migrationClass, containmentMigrationClassList,objectMap);
		}
		if (migrationClass.getAttributeCollection() != null
				&& !migrationClass.getAttributeCollection().isEmpty())
		{
			Collection<Attribute> attributes = migrationClass
					.getAttributeCollection();
			processAttributes(mainObj, migrationClass, attributes);
		}
	}

	/**
	 * @param mainObj
	 * @param mainObjectClass
	 * @param containments
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws ClassNotFoundException 
	 * @throws InstantiationException 
	 */
	private void processContainments(Object mainObj, MigrationClass mainMigrationClass,
			Collection<MigrationClass> containmentMigrationClassList, ObjectIdentifierMap mainObjectIdentifierMap) throws MigrationException, InstantiationException, IllegalAccessException
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
				List sortedList = new ArrayList(containmentObjectCollection); 
				Collections.sort(sortedList, new SortObject());
				containmentObjectCollection = new LinkedHashSet(sortedList);
				

				//added for participant race
				Collection<Object> newContainmentObjectCollection = new LinkedHashSet<Object>();

				if (containmentObjectCollection != null)
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
							Long sandBoxId = containmentMigrationClass.invokeGetIdMethod(containmentObject);
							ObjectIdentifierMap containIdentifierMap = mainObjectIdentifierMap.createOldContainmentObjectIdentifierMap(containmentMigrationClass.getRoleName(), sandBoxId, containmentMigrationClass.getClassName());
							processObject(containmentObject, containmentMigrationClass,containIdentifierMap);
							//detaching the object from the session
							//SandBoxDao.getCurrentSession().evict(containmentObject);
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
						Long sandBoxId = containmentMigrationClass.invokeGetIdMethod(containmentObject);
						ObjectIdentifierMap containIdentifierMap = mainObjectIdentifierMap.createOldContainmentObjectIdentifierMap(containmentMigrationClass.getRoleName(), sandBoxId, containmentMigrationClass.getClassName());
						processObject(containmentObject, containmentMigrationClass,containIdentifierMap);
						containmentMigrationClass.invokeSetIdMethod(containmentObject, null);
					}
					String roleName = containmentMigrationClass.getRoleName();
					//detaching the object from the session
					//SandBoxDao.getCurrentSession().evict(containmentObject);
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{containmentObject.getClass()}, mainObj, containmentObject);
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
				if (associationObjectCollection != null)
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
							//processObject(associatedObject, associationMigrationClass);
							//get id must of the persistent object
							//Method getId = persitentObj.getClass().getMethod("getId", null);
							//Object id = getId.invoke(persitentObj, null);
							Long sandBoxId = associationMigrationClass.invokeGetIdMethod(associatedObject);
							Long productionId = SandBoxDao.getProductionId(sandBoxId, associationMigrationClass.getClassName());
							
							// setid must be of the new object class
							Object newAssociatedObject = associationMigrationClass.getNewInstance(); 
							
							//associatedClass.newInstance();
							associationMigrationClass.invokeSetIdMethod(newAssociatedObject, productionId);
							
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
				//processObject(associatedObject, associationMigrationClass);
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
					}
						String roleName = associationMigrationClass.getRoleName();
						mainMigrationClass.invokeSetterMethod(roleName, new Class[]{associatedObject.getClass()},mainObj, newAssociatedObject);
				}
			}
		}
	}
	/**
	 * 
	 * @param mainObj
	 * @param mainMigrationClass
	 * @param attributes
	 * @throws MigrationException
	 */
	private void processAttributes(Object mainObj, MigrationClass mainMigrationClass,
			Collection<Attribute> attributes) throws MigrationException
	{
		Iterator<Attribute> attributeItertor = attributes.iterator();
		while (attributeItertor.hasNext())
		{
			Attribute attribute = attributeItertor.next();
			String isToSetNull = attribute.getIsToSetNull();

			Object attributeObject = mainMigrationClass.invokeGetterMethod(attribute.getName(),
					null, mainObj, null);
			
				if (!"Yes".equalsIgnoreCase(isToSetNull))
				{
					try
					{
						Class dataTypeClass = Class.forName(attribute.getDataType());
						Constructor constructor = dataTypeClass.getConstructor(Class.forName("java.lang.String"));
						Object setObject = constructor.newInstance(attribute.getValueToSet()); 
							//dataTypeClass.cast(attribute.getValueToSet());
						//Method convertoPrimitive = setObject.getClass().getMethod("booleanValue", null);
						//Object o =convertoPrimitive.invoke(setObject, null);
						mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{setObject.getClass()}, mainObj, setObject);
					}
					catch (Exception e)
					{
						Boolean b = new Boolean("false");
						e.printStackTrace();
					}
				}
				else
				{
					mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{attributeObject
						.getClass()}, mainObj, null);
				}
			
		}
	}
	
}