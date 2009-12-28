
package edu.wustl.bulkoperator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import edu.wustl.bulkoperator.appservice.MigrationAppService;
import edu.wustl.bulkoperator.jobmanager.JobData;
import edu.wustl.bulkoperator.metadata.Attribute;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.ObjectIdentifierMap;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.global.Validator;

public class BulkOperationProcessor
{

	MigrationAppService migrationAppService;
	BulkOperationClass bulkOperationclass = null;
	ObjectIdentifierMap objectMap;
	DataList dataList = null;
	int counter = 0;
	boolean isUpdateOperation = false;
	int currentRowIndex = 0;
	JobData jobData = null;

	public BulkOperationProcessor(BulkOperationClass migration,
			MigrationAppService migrationAppService, DataList list, JobData jobData)
	{
		this.bulkOperationclass = migration;
		this.migrationAppService = migrationAppService;
		this.dataList = list;
		this.jobData = jobData;
		isUpdateOperation = bulkOperationclass.isUpdateOperation();
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
	public void process() throws BulkOperationException
	{
		int failureCount = 0;
		int successCount = 0;
		int modValue = bulkOperationclass.getBatchSize();
		if(modValue == 0)
		{
			modValue = 100;
		}
		try
		{
			for (int i = 0; i < dataList.size(); i++)
			{
				try
				{
					Hashtable<String, String> valueTable = dataList.getValue(i);
					currentRowIndex = i;

					if (isUpdateOperation)
					{
						String hql = BulkOperationUtility.createHQL(bulkOperationclass, dataList
								.getValue(currentRowIndex));
						Object searchedObject = migrationAppService.search(hql);
						if(searchedObject == null)
						{
							throw new Exception("Object not found in database.");
						}
						else
						{
							processObject(searchedObject, bulkOperationclass, "");
							migrationAppService.update(searchedObject);
						}
					}
					else
					{
						Object domainObject = bulkOperationclass.getClassDiscriminator(valueTable);
						if(domainObject == null)
						{
							domainObject = bulkOperationclass.getNewInstance();
						}
						processObject(domainObject, bulkOperationclass, "");
 						migrationAppService.insert(domainObject, bulkOperationclass, objectMap);
					}
					dataList.addStatusMessage(currentRowIndex, "Success", " ");
					successCount++;
				}
				catch (BulkOperationException exp)
				{
					failureCount++;
					insertReportInDatabase(currentRowIndex, failureCount, JobData.JOB_FAILED_STATUS);
					throw new BulkOperationException(exp);
				}
				catch (Exception exp)
				{
					dataList.addStatusMessage(currentRowIndex, "Failure", exp.getMessage());
					failureCount++;
				}
				if ((currentRowIndex % modValue) == 0)
				{	
					insertReportInDatabase(successCount, failureCount, JobData.JOB_IN_PROGRESS_STATUS);
				}
			}
			insertReportInDatabase(dataList.size(), failureCount, JobData.JOB_COMPLETED_STATUS);			
		}
		catch (Exception exp)
		{
			exp.printStackTrace();
			throw new BulkOperationException(exp.getMessage(), exp);
		}
	}

	/**
	 * @param i
	 * @throws IOException
	 */
	private void insertReportInDatabase(int recordsProcessed, int failureCount, String statusMessage)
	throws IOException
	{
		BulkOperationUtility utility = new BulkOperationUtility();
		String commonFileName = bulkOperationclass.getTemplateName() + jobData.getJobID();
		File file = dataList.createCSVReportFile(commonFileName);
		String [] fileNames = file.getName().split(".csv");
		String zipFilePath = CommonServiceLocator.getInstance().getAppHome()
					+ System.getProperty("file.separator") + fileNames[0];
		File zipFile = utility.createZip(file, zipFilePath);
		Timestamp startedTime = jobData.getStartedTime();
		Timestamp currentTime = new Timestamp(System.currentTimeMillis());
		long timeTaken = 0L;
		int hours = currentTime.getHours() - startedTime.getHours();
		if(hours > 0 )
		{
			timeTaken = hours * 60 * 60; 
		}
		int minutes = currentTime.getMinutes() - startedTime.getHours();
		if(minutes > 0)
		{
			timeTaken = timeTaken + (minutes * 60);
		}
		int seconds = currentTime.getMinutes() - startedTime.getHours();
		timeTaken = timeTaken + seconds;
		Object[] keys = {JobData.LOG_FILE_KEY,
				JobData.NO_OF_RECORDS_PROCESSED_KEY, JobData.NO_OF_FAILED_RECORDS_KEY,
				JobData.TIME_TAKEN_KEY, JobData.NO_OF_TOTAL_RECORDS_KEY, JobData.LOG_FILE_NAME_KEY};
		Object[] values = {zipFile, recordsProcessed, failureCount, timeTaken, dataList.size(), zipFile.getName()};
		jobData.updateJobStatus(keys, values, statusMessage);
		zipFile.delete();
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
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws ClassNotFoundException 
	 */
	public void processObject(Object mainObj, BulkOperationClass migrationClass, String columnSuffix)
			throws Exception
	{
		if (migrationClass.getAttributeCollection() != null
				&& !migrationClass.getAttributeCollection().isEmpty())
		{
			processAttributes(mainObj, migrationClass, columnSuffix);
		}

		if (migrationClass.getContainmentAssociationCollection() != null
				&& !migrationClass.getContainmentAssociationCollection().isEmpty())
		{
			processContainments(mainObj, migrationClass, columnSuffix);
		}

		if (migrationClass.getReferenceAssociationCollection() != null
				&& !migrationClass.getReferenceAssociationCollection().isEmpty())
		{
			processAssociations(mainObj, migrationClass, columnSuffix);
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
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws ClassNotFoundException 
	 */
	private void processContainments(Object mainObj, BulkOperationClass mainMigrationClass,
			String columnSuffix) throws BulkOperationException
	{

		try
		{
			Iterator<BulkOperationClass> containmentItert = mainMigrationClass
					.getContainmentAssociationCollection().iterator();
			while (containmentItert.hasNext())
			{
				BulkOperationClass containmentMigrationClass = containmentItert.next();
				String cardinality = containmentMigrationClass.getCardinality();
				if (cardinality != null && cardinality.equals("*") && cardinality != "")
				{
					Collection containmentObjectCollection = (Collection) mainMigrationClass
							.invokeGetterMethod(containmentMigrationClass.getRoleName(), null,
									mainObj, null);
					if (containmentObjectCollection == null)
					{
						containmentObjectCollection = new LinkedHashSet();
					}
					List sortedList = new ArrayList(containmentObjectCollection);
					//Collections.sort(sortedList, new SortObject());
					containmentObjectCollection = new LinkedHashSet(sortedList);
					//added for participant race
					//Collection<Object> newContainmentObjectCollection = new LinkedHashSet<Object>();
					//create a containment obj and populate data in it from CSV and then add 
					//it to NewContainmentCollection
					int maxNoOfRecords = containmentMigrationClass.getMaxNoOfRecords().intValue();
					for (int i = 1; i <= maxNoOfRecords; i++)
					{
						List<String> attributeList = BulkOperationUtility.getAttributeList(
								containmentMigrationClass, "#" + i);
						if (dataList.checkIfAtLeastOneColumnHasAValue(currentRowIndex,
								attributeList))
						{
							Object containmentObject = containmentMigrationClass.getNewInstance();
							processObject(containmentObject, containmentMigrationClass, "#" + i);
							containmentObjectCollection.add(containmentObject);
							String roleName = containmentMigrationClass.getParentRoleName();
							if (!Validator.isEmpty(roleName))
							{
								containmentMigrationClass.invokeSetterMethod(roleName,
										new Class[]{mainObj.getClass()},
										containmentObject, mainObj);
							}
						}
					}
					String roleName = containmentMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{Collection.class},
							mainObj, containmentObjectCollection);
				}
				else if (cardinality != null && cardinality.equals("1") && cardinality != "")
				{
					Object containmentObject = mainMigrationClass.invokeGetterMethod(
							containmentMigrationClass.getRoleName(), null, mainObj, null);
					if (containmentObject == null)
					{
						Class klass = containmentMigrationClass.getClassObject();
						Constructor constructor = klass.getConstructor(null);
						containmentObject = constructor.newInstance();
					}
					processObject(containmentObject, containmentMigrationClass, columnSuffix);
					String roleName = containmentMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{containmentObject
							.getClass()}, mainObj, containmentObject);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new BulkOperationException(e.getMessage(), e);
		}
	}

	/**
	 * @param mainObj
	 * @param mainObjectClass
	 * @param associationsMigrationClassList
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 */
	private void processAssociations(Object mainObj, BulkOperationClass mainMigrationClass,
			String columnSuffix) throws BulkOperationException
	{
		try
		{
			Iterator<BulkOperationClass> associationItert = mainMigrationClass
					.getReferenceAssociationCollection().iterator();
			while (associationItert.hasNext())
			{
				// the associated object to the main object
				BulkOperationClass associationMigrationClass = associationItert.next();
				//the function name related to the associated object used in the main object
				String cardinality = associationMigrationClass.getCardinality();
				//Class<?> associatedClass = Class.forName(associationMigrationClass.getClassName());
				if (cardinality != null && cardinality.equals("*") && cardinality != ""
						&& mainObj != null)
				{
					Collection<Object> newAssociationCollection = new LinkedHashSet<Object>();
					Collection associationObjectCollection = (Collection) mainMigrationClass
							.invokeGetterMethod(associationMigrationClass.getRoleName(), null,
									mainObj, null);
					//getterForAssociation.invoke(mainObj, null);
					if (associationObjectCollection == null)
					{
						associationObjectCollection = newAssociationCollection;
					}
					if (associationObjectCollection != null)
					{
						String roleName = associationMigrationClass.getRoleName();
						mainMigrationClass.invokeSetterMethod(roleName,
								new Class[]{Collection.class}, mainObj, newAssociationCollection);
					}
				}
				else if (cardinality != null && cardinality.equals("1") && cardinality != "")
				{
					Object associatedObject = mainMigrationClass.invokeGetterMethod(
							associationMigrationClass.getRoleName(), null, mainObj, null);

					if (associatedObject == null)
					{
						associatedObject = associationMigrationClass.getNewInstance();
					}
					Collection<Attribute> attributes = associationMigrationClass
							.getAttributeCollection();
					//added for setting the old values to the object
					processObject(associatedObject, associationMigrationClass, columnSuffix);
					String roleName = associationMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{associatedObject
							.getClass()}, mainObj, associatedObject);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new BulkOperationException(e.getMessage(), e);
		}
	}

	/**
	 * 
	 * @param mainObj
	 * @param mainMigrationClass
	 * @param attributes
	 * @throws BulkOperationException
	 */
	private void processAttributes(Object mainObj, BulkOperationClass mainMigrationClass,
			String columnSuffix) throws Exception
	{
		try
		{
			Iterator<Attribute> attributeItertor = mainMigrationClass.getAttributeCollection()
					.iterator();
			Hashtable<String, String> valueTable = dataList.getValue(currentRowIndex);
			while (attributeItertor.hasNext())
			{
				Attribute attribute = attributeItertor.next();
				if (attribute.getDataType() != null && !"".equals(attribute.getDataType()))
				{
					if(String.valueOf(mainObj.getClass()).contains(attribute.getBelongsTo()))
					{
						Class dataTypeClass = Class.forName(attribute.getDataType());
						if (!Validator.isEmpty(attribute.getCsvColumnName() + columnSuffix))
						{
							if (!Validator.isEmpty(valueTable.get(attribute.getCsvColumnName()
									+ columnSuffix)))
							{
								String csvData = valueTable.get(attribute.getCsvColumnName()
										+ columnSuffix);
								Object attributeValue = attribute.getValueOfDataType(csvData);
								mainMigrationClass.invokeSetterMethod(attribute.getName(),
										new Class[]{dataTypeClass}, mainObj, attributeValue);
							}
						}
						else
						{
							throw new BulkOperationException("bulk.error.csv.column.name.change");
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new Exception(e.getMessage(), e);
		}
	}
}