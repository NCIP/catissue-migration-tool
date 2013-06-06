/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue-migration-tool/LICENSE.txt for details.
 */

package edu.wustl.migrator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.wustl.migrator.appservice.MigrationAppService;
import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.Attribute;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationException;
import edu.wustl.migrator.util.MigrationUtility;
import edu.wustl.migrator.util.SortIds;
import edu.wustl.migrator.util.UnMigratedObject;

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

	Set<Long> ids = new LinkedHashSet<Long>();

	public Set<Long> getIds()
	{
		return ids;
	}

	public void setIds(Set<Long> ids)
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
		List list = null;
		ids.clear();
		String unMigratedObjectFlag = Migrator.unMigratedObjectFlag;
//MigrationProperties.getValue(MigrationConstants.unmigratedObjectflag);
		if(unMigratedObjectFlag != null && unMigratedObjectFlag != "")
		{
			String whereColumn[] = new String[1];
			whereColumn[0] = new String("className");
			String whereValue[] = new String[1];
			whereValue[0] = new String("'"+migrationClass.getClassName()+"'");
			String returnCoulmn[] = new String[1];
			returnCoulmn[0] = new String("sandBoxId");
		list = SandBoxDao.retrieve(UnMigratedObject.class.getName(), whereColumn, whereValue, returnCoulmn);
		}
		else
		{
		list = SandBoxDao.executeSQLQuery(migrationClass.getSql());
		}
		if(list != null && !list.isEmpty())
		{
			Object obj = list.get(0);
			if(obj instanceof Long)
			{
				ids.addAll(list);
			}
			else
			{
				Iterator i = list.iterator();
				while(i.hasNext())
				{
					Long id = Long.valueOf(i.next().toString());
					ids.add(id);
				}
			}
		}
		//ids.addAll(list);
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
		try
		{
			Map<Object, Object> sandBoxObjs = new HashMap<Object, Object>();
			
			//List<Object> listForInsertion = new ArrayList<Object>();
		
			if(SandBoxDao.getCurrentSession() == null)
			{
				SandBoxDao.getNewSession();
			}
			fetchObjectIdentifier();
			String className = migrationClass.getClassName();
			String query = null;
			List<Object> objectList = null;
			/*for(int i=0;i<ids.size();i++)
			{*/
			if(ids != null && !ids.isEmpty())
			{
				List l = new LinkedList();
				l.addAll(ids);
				Collections.sort(l, new SortIds());
				Iterator idIterator = l.iterator();
				Long t1 = MigrationUtility.getTime();
				while (idIterator.hasNext())
				{
					query = "from " + migrationClass.getClassName() + " where id = " + idIterator.next().toString();
					System.out.println("query"+query);
					objectList = SandBoxDao.executeHQLQuery(query.toString());
					
					if (objectList != null && !objectList.isEmpty())
					{
						Object object = objectList.get(0);
						objectMap = new ObjectIdentifierMap(migrationClass.getClassName());
						objectMap.setOldId(migrationClass.invokeGetIdMethod(object));

						processObject(object, migrationClass, objectMap);
						System.out.println(object);
						migrationAppService.insert(object, migrationClass, objectMap);
						//listForInsertion.add(object);
					}
					
				}
				Long t2 = MigrationUtility.getTime();
				Long totalTime = t2 - t1 ;
				System.out.println("time taken for migration of "+migrationClass.getClassName()+" = " + totalTime + "seconds");
				if(totalTime > 60)
				{
					System.out.println("time taken for migration of "+migrationClass.getClassName()+" = " + totalTime/60 + "mins");
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
			processAttributes(mainObj, migrationClass, attributes, null);
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
							Long sandBoxId = associationMigrationClass.invokeGetIdMethod(associatedObject);
							Long productionId = SandBoxDao.getProductionId(sandBoxId, associationMigrationClass.getClassName());
							
							
							//temp to remove
							if(productionId == null)
							{
								productionId = sandBoxId;
							}
							//end of to remove	
								
								
							// setid must be of the new object class
							Object newAssociatedObject = associationMigrationClass.getNewInstance(); 
							associationMigrationClass.invokeSetIdMethod(newAssociatedObject, productionId);
							
							Collection<Attribute> attributes = associationMigrationClass.getAttributeCollection();
							//added for setting the old values to the object
							processAttributes(newAssociatedObject, associationMigrationClass, attributes, associatedObject);
							
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
						
						//temp to remove
						if(productionId == null)
						{
							productionId = sandBoxId;
						}
						//end of to remove	
						
						newAssociatedObject = associationMigrationClass.getNewInstance();
						//setting the production id to the new object
						associationMigrationClass.invokeSetIdMethod(newAssociatedObject, productionId);
						Collection<Attribute> attributes = associationMigrationClass.getAttributeCollection();
						//added for setting the old values to the object
						processAttributes(newAssociatedObject, associationMigrationClass, attributes, associatedObject);
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
			Collection<Attribute> attributes, Object oldObject) throws MigrationException
	{
		Iterator<Attribute> attributeItertor = attributes.iterator();
		while (attributeItertor.hasNext())
		{
			Attribute attribute = attributeItertor.next();
			String isToSetNull = attribute.getIsToSetNull();
			Object attributeObject = null;
			if(oldObject != null)
			{
				attributeObject = mainMigrationClass.invokeGetterMethod(attribute.getName(),
						null, oldObject, null);
			}
			else
			{
				attributeObject = mainMigrationClass.invokeGetterMethod(attribute.getName(),
						null, mainObj, null);
			}
			if(attributeObject instanceof Collection || attributeObject instanceof Long)
			{
				if (!"Yes".equalsIgnoreCase(isToSetNull))
				{
					try
					{
						String dataType = attribute.getDataType();
						Class dataTypeClass = Class.forName(dataType);
						Object value = attribute.getValueToSet();
						Object setObject =  null;
						Constructor constructor = null;
						if("".equals(dataType))
						{
							constructor = dataTypeClass.getConstructor(Class.forName("java.lang.String"));
							setObject = constructor.newInstance(value);
							mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{dataTypeClass}, mainObj, setObject);
						}
						else
						{
							Class[] ctorArgs1 = new Class[1];
				            ctorArgs1[0] = dataTypeClass;				            
							Object newAttributeObj = dataTypeClass.newInstance();
							setObject = mainMigrationClass.invokeGetterMethod(attribute.getName(), null, mainObj, null);
							if(newAttributeObj instanceof Collection)
							{
								Collection<String> collection = (Collection) setObject;
								Iterator it = collection.iterator();
								while(it.hasNext())
								{
									((Collection)newAttributeObj).add(it.next());
								}
								mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{java.util.Collection.class}, mainObj, (Collection)newAttributeObj);
							}
							else
							{
								mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{dataTypeClass}, mainObj, setObject);
							}
						}
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				else
				{
					mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{attributeObject
						.getClass()}, mainObj, new Class[]{null});
				}
			}
			else
			{
				if (!"Yes".equalsIgnoreCase(isToSetNull))
				{
					try
					{
						Class dataTypeClass = Class.forName(attribute.getDataType());
						Object value = attribute.getValueToSet();
						Object setObject =  null;
						Constructor constructor = null;
						if(value != null && value.toString() != "")
						{
							constructor = dataTypeClass.getConstructor(Class.forName("java.lang.String"));
							setObject = constructor.newInstance(value); 
						}
						else
						{
							//constructor = dataTypeClass.getConstructor(Class.forName("java.lang.String"));
							setObject = mainMigrationClass.invokeGetterMethod(attribute.getName(), null, oldObject, null);
						}
						mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{setObject.getClass()}, mainObj, setObject);
					}
					catch (Exception e)
					{
						
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
}