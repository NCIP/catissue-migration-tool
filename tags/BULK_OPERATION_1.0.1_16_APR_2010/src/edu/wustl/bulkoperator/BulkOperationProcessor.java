
package edu.wustl.bulkoperator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.global.Validator;
import edu.wustl.common.util.logger.Logger;

public class BulkOperationProcessor
{
	/**
	 * logger.
	 */
	private transient final Logger logger = Logger.getCommonLogger(BulkOperationProcessor.class);
	MigrationAppService migrationAppService;
	BulkOperationClass bulkOperationclass = null;
	ObjectIdentifierMap objectMap;
	DataList dataList = null;
	int counter = 0;
	long timetaken = System.currentTimeMillis()/1000;
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
							throw new Exception("Could not find the specified data in the database.");
						}
						else
						{
							processObject(searchedObject, bulkOperationclass, "", false);
							migrationAppService.update(searchedObject);
						}
					}
					else
					{
						Object domainObject = bulkOperationclass.getClassDiscriminator(valueTable, "");
						if(domainObject == null)
						{
							domainObject = bulkOperationclass.getNewInstance();
						}
						processObject(domainObject, bulkOperationclass, "", false);
 						migrationAppService.insert(domainObject, bulkOperationclass, objectMap);
					}
					dataList.addStatusMessage(currentRowIndex, "Success", " ");
					successCount++;
				}
				catch (BulkOperationException exp)
				{
					failureCount++;
					dataList.addStatusMessage(currentRowIndex, "Failure", " "+exp.getMessage());
				}
				catch (Exception exp)
				{
					dataList.addStatusMessage(currentRowIndex, "Failure", " "+exp.getMessage());
					failureCount++;
				}
				if ((currentRowIndex % modValue) == 0)
				{	
					insertReportInDatabase(successCount, failureCount, JobData.JOB_IN_PROGRESS_STATUS);
				}
			}
			insertReportInDatabase(successCount, failureCount, JobData.JOB_COMPLETED_STATUS);			
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
	}

	/**
	 * @param i
	 * @throws IOException
	 */
	private void insertReportInDatabase(int recordsProcessed, int failureCount, String statusMessage)
		throws BulkOperationException
	{
		try
		{
			BulkOperationUtility utility = new BulkOperationUtility();
			String commonFileName = bulkOperationclass.getTemplateName() + jobData.getJobID();
			File file = dataList.createCSVReportFile(commonFileName);
			String [] fileNames = file.getName().split(".csv");
			String zipFilePath = CommonServiceLocator.getInstance().getAppHome()
						+ System.getProperty("file.separator") + fileNames[0];
			File zipFile = utility.createZip(file, zipFilePath);
			long localTimetaken = (System.currentTimeMillis()/1000) - timetaken;
			Object[] keys = {JobData.LOG_FILE_KEY,
					JobData.NO_OF_RECORDS_PROCESSED_KEY, JobData.NO_OF_FAILED_RECORDS_KEY,
					JobData.TIME_TAKEN_KEY, JobData.NO_OF_TOTAL_RECORDS_KEY, JobData.LOG_FILE_NAME_KEY};
			Object[] values = {zipFile, recordsProcessed, failureCount, localTimetaken, dataList.size(), zipFile.getName()};
			jobData.updateJobStatus(keys, values, statusMessage);
			zipFile.delete();
		}
		catch (BulkOperationException exp)
		{
			logger.error(exp.getMessage(), exp);
			throw new BulkOperationException(exp.getErrorKey(), exp, exp.getMsgValues());
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
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws ClassNotFoundException 
	 */
	public void processObject(Object mainObj, BulkOperationClass migrationClass,
			String columnSuffix, boolean validate) throws BulkOperationException
	{
		if (migrationClass.getAttributeCollection() != null
				&& !migrationClass.getAttributeCollection().isEmpty())
		{
			processAttributes(mainObj, migrationClass, columnSuffix, validate);
		}

		if (migrationClass.getContainmentAssociationCollection() != null
				&& !migrationClass.getContainmentAssociationCollection().isEmpty())
		{
			processContainments(mainObj, migrationClass, columnSuffix, validate);
		}

		if (migrationClass.getReferenceAssociationCollection() != null
				&& !migrationClass.getReferenceAssociationCollection().isEmpty())
		{
			processAssociations(mainObj, migrationClass, columnSuffix, validate);
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
		String columnSuffix, boolean validate)
		throws BulkOperationException
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
					containmentObjectCollection = new LinkedHashSet(sortedList);
					int maxNoOfRecords = containmentMigrationClass.getMaxNoOfRecords().intValue();
					for (int i = 1; i <= maxNoOfRecords; i++)
					{
						List<String> attributeList = BulkOperationUtility.getAttributeList(
								containmentMigrationClass, columnSuffix + "#" + i);
						if (dataList.checkIfAtLeastOneColumnHasAValue(currentRowIndex,
								attributeList) || validate)
						{
							Object containmentObject = containmentMigrationClass.getClassDiscriminator(
									dataList.getValue(currentRowIndex), columnSuffix + "#" + i);//getNewInstance();
							if(containmentObject == null)
							{
								if("java.lang.String".equals(containmentMigrationClass.getClassName()))
								{
									containmentObject = new StringBuffer();
								}
								else
								{
									containmentObject = containmentMigrationClass.getNewInstance();
								}
							}
							processObject(containmentObject, containmentMigrationClass,
									columnSuffix + "#" + i, validate);
							if("java.lang.String".equals(containmentMigrationClass.getClassName()))
							{
								containmentObjectCollection.add(containmentObject.toString());
							}
							else
							{
								containmentObjectCollection.add(containmentObject);
							}
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
					List<String> attributeList = BulkOperationUtility.getAttributeList(
							containmentMigrationClass, columnSuffix);
					if (dataList.checkIfAtLeastOneColumnHasAValue(currentRowIndex,
							attributeList) || validate)
					{
						Object containmentObject = mainMigrationClass.invokeGetterMethod(
							containmentMigrationClass.getRoleName(), null, mainObj, null);										
						if (containmentObject == null)
						{
							containmentObject = containmentMigrationClass.getClassDiscriminator(
									dataList.getValue(currentRowIndex), columnSuffix);
						}
						if (containmentObject == null)
						{
							Class klass = containmentMigrationClass.getClassObject();
							Constructor constructor = klass.getConstructor(null);
							containmentObject = constructor.newInstance();
						}
						processObject(containmentObject, containmentMigrationClass,
								columnSuffix, validate);
						String roleName = containmentMigrationClass.getRoleName();
						mainMigrationClass.invokeSetterMethod(roleName, new Class[]{containmentObject
								.getClass()}, mainObj, containmentObject);
					}
				}
			}
		}
		catch (BulkOperationException bulkExp)
		{
			logger.error(bulkExp.getMessage(), bulkExp);
			throw new BulkOperationException(bulkExp.getErrorKey(), bulkExp, bulkExp.getMsgValues());
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
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
		String columnSuffix, boolean validate)
		throws BulkOperationException
	{
		try
		{
			Iterator<BulkOperationClass> associationItert = mainMigrationClass
					.getReferenceAssociationCollection().iterator();
			while (associationItert.hasNext())
			{
				BulkOperationClass associationMigrationClass = associationItert.next();
				String cardinality = associationMigrationClass.getCardinality();
				if (cardinality != null && cardinality.equals("*") && cardinality != ""
						&& mainObj != null)
				{
					Collection associationObjectCollection = (Collection) mainMigrationClass
							.invokeGetterMethod(associationMigrationClass.getRoleName(), null,
									mainObj, null);
					if (associationObjectCollection == null)
					{
						associationObjectCollection = new LinkedHashSet<Object>();
					}
					List sortedList = new ArrayList(associationObjectCollection);
					associationObjectCollection = new LinkedHashSet(sortedList);
					int maxNoOfRecords = associationMigrationClass.getMaxNoOfRecords().intValue();
					for (int i = 1; i <= maxNoOfRecords; i++)
					{
						List<String> attributeList = BulkOperationUtility.getAttributeList(
								associationMigrationClass, columnSuffix + "#" + i);
						if (dataList.checkIfAtLeastOneColumnHasAValue(currentRowIndex,
								attributeList) || validate)
						{
							Object referenceObject = associationMigrationClass.getClassDiscriminator(
									dataList.getValue(currentRowIndex), columnSuffix + "#" + i);
							if(referenceObject == null)
							{
								if("java.lang.String".equals(associationMigrationClass.getClassName()))
								{
									referenceObject = new StringBuffer();
								}
								else
								{
									referenceObject = associationMigrationClass.getNewInstance();
								}
							}
							processObject(referenceObject, associationMigrationClass,
									columnSuffix + "#" + i, validate);
							if("java.lang.String".equals(associationMigrationClass.getClassName()))
							{
								associationObjectCollection.add(referenceObject.toString());
							}
							else
							{
								associationObjectCollection.add(referenceObject);
							}
							String roleName = associationMigrationClass.getParentRoleName();
							if (!Validator.isEmpty(roleName))
							{
								associationMigrationClass.invokeSetterMethod(roleName,
										new Class[]{mainObj.getClass()},
										referenceObject, mainObj);
							}
						}
					}	
					String roleName = associationMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{Collection.class},
							mainObj, associationObjectCollection);
				}
				else if (cardinality != null && cardinality.equals("1") && cardinality != "")
				{
					List<String> attributeList = BulkOperationUtility.getAttributeList(
							associationMigrationClass, columnSuffix);
					if (dataList.checkIfAtLeastOneColumnHasAValue(currentRowIndex,
							attributeList) || validate)
					{
						Object associatedObject = mainMigrationClass.invokeGetterMethod(
								associationMigrationClass.getRoleName(), null, mainObj, null);	
						if (associatedObject == null)
						{
							associatedObject = associationMigrationClass.getClassDiscriminator(
									dataList.getValue(currentRowIndex), columnSuffix);
						}
						if (associatedObject == null)
						{
							Class klass = associationMigrationClass.getClassObject();
							Constructor constructor = klass.getConstructor(null);
							associatedObject = constructor.newInstance();
						}
						processObject(associatedObject, associationMigrationClass,
								columnSuffix, validate);
						String roleName = associationMigrationClass.getRoleName();
						mainMigrationClass.invokeSetterMethod(roleName, new Class[]{associatedObject
								.getClass()}, mainObj, associatedObject);
					}
				}
			}
		}
		catch (BulkOperationException bulkExp)
		{
			logger.error(bulkExp.getMessage(), bulkExp);
			throw new BulkOperationException(bulkExp.getErrorKey(), bulkExp, bulkExp.getMsgValues());
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
	}

	/**
	 * 
	 * @param mainObj
	 * @param mainMigrationClass
	 * @param columnSuffix
	 * @param validate
	 * @throws BulkOperationException
	 */
	private void processAttributes(Object mainObj, BulkOperationClass mainMigrationClass,
		String columnSuffix, boolean validate)
		throws BulkOperationException
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
					if (valueTable.get(attribute.getCsvColumnName() + columnSuffix) != null)
					{
						if("java.lang.String".equals(mainMigrationClass.getClassName()))
						{
							if (!Validator.isEmpty(valueTable.get(attribute.getCsvColumnName()
									+ columnSuffix)))
							{
								String csvData = valueTable.get(attribute.getCsvColumnName()
										+ columnSuffix);
								Object attributeValue = attribute.getValueOfDataType(csvData, validate);
								((StringBuffer)mainObj).append(attributeValue);
							}
						}
						else if("java.lang.Long".equals(mainMigrationClass.getClassName()) ||
									"java.lang.Double".equals(mainMigrationClass.getClassName()) ||
									"java.lang.Integer".equals(mainMigrationClass.getClassName()) ||
									"java.lang.Boolean".equals(mainMigrationClass.getClassName()) ||
									"java.lang.Float".equals(mainMigrationClass.getClassName()))
						{	
							if (!Validator.isEmpty(valueTable.get(attribute.getCsvColumnName()
									+ columnSuffix)))
							{
								String csvData = valueTable.get(attribute.getCsvColumnName() + columnSuffix);
								mainObj = csvData;
							}
						}
						else if(String.valueOf(mainObj.getClass()).contains(attribute.getBelongsTo()))
						{
							Class dataTypeClass = Class.forName(attribute.getDataType());
							if (!Validator.isEmpty(valueTable.get(
									attribute.getCsvColumnName() + columnSuffix)))
							{
								String csvData = valueTable.get(attribute.getCsvColumnName()
										+ columnSuffix);
								Object attributeValue = attribute.getValueOfDataType(csvData, validate);
								mainMigrationClass.invokeSetterMethod(attribute.getName(),
										new Class[]{dataTypeClass}, mainObj, attributeValue);
							}
						}//else if ends
					}
					else
					{
						throwExceptionForColumnNameNotFound(mainMigrationClass, validate,
								attribute);
					}//null check if - else ends
				}//data type null check ends
			}//while ends
		}
		catch (BulkOperationException bulkExp)
		{
			logger.error(bulkExp.getMessage(), bulkExp);
			throw new BulkOperationException(bulkExp.getErrorKey(), bulkExp, bulkExp.getMsgValues());
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
	}

	/**
	 * 
	 * @param mainMigrationClass
	 * @param validate
	 * @param attribute
	 * @throws BulkOperationException
	 */
	private void throwExceptionForColumnNameNotFound(BulkOperationClass mainMigrationClass,
			boolean validate, Attribute attribute) throws BulkOperationException
	{
		if(validate)
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.column.name.change.validation");
			throw new BulkOperationException(errorkey, null, 
				attribute.getCsvColumnName() + ":" + attribute.getName() + ":" + mainMigrationClass.getClassName());
		}
		else
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.column.name.change");
			throw new BulkOperationException(errorkey, null, "");
		}
	}
}