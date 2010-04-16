
package edu.wustl.bulkoperator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.jobmanager.JobData;
import edu.wustl.bulkoperator.metadata.Attribute;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
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
	private static final Logger logger = Logger.getCommonLogger(BulkOperationProcessor.class);
	private transient final AbstractBulkOperationAppService bulkOprAppService;
	private transient BulkOperationClass bulkOperationclass = null;
	private transient BulkOperationClass DEBulkOperationClass = null;
	private transient DataList dataList = null;
	private transient long timetaken = System.currentTimeMillis() / 1000;
	private transient boolean isUpdateOperation = false;
	private transient int currentRowIndex = 0;
	private transient JobData jobData = null;
	private transient BulkOperationUtility bulkOperationUtility = null;

	/**
	 * 
	 * @param bulkOperationClass
	 * @param bulkOprAppService
	 * @param list
	 * @param jobData
	 */
	public BulkOperationProcessor(BulkOperationClass bulkOperationClass,
			AbstractBulkOperationAppService bulkOprAppService, DataList list, JobData jobData)
	{
		this.bulkOperationclass = bulkOperationClass;
		this.bulkOprAppService = bulkOprAppService;
		this.dataList = list;
		this.jobData = jobData;
		isUpdateOperation = bulkOperationclass.isUpdateOperation();
		bulkOperationUtility = new BulkOperationUtility();
		DEBulkOperationClass = bulkOperationUtility.checkForDEObject(bulkOperationClass);
	}

	/**
	 * 
	 * @throws BulkOperationException
	 */
	public void process() throws BulkOperationException
	{
		int failureCount = 0;
		int successCount = 0;
		int modValue = bulkOperationclass.getBatchSize();
		if (modValue == 0)
		{
			modValue = 100;
		}
		try
		{
			for (int i = 0; i < dataList.size(); i++)
			{
				Object staticObject = null;
				Object DEObject = null;
				Long staticObjectId;
				Long dynExtObjectId;
				String resultIds = "";
				try
				{
					Map<String, String> valueTable = dataList.getValue(i);
					currentRowIndex = i;

					if (isUpdateOperation)
					{
						if(DEBulkOperationClass == null)
						{
							String hql = bulkOperationUtility.createHQL(bulkOperationclass, dataList
									.getValue(currentRowIndex));
							staticObject = bulkOprAppService.search(hql);
							if (staticObject == null)
							{
								throw new Exception(
										"Could not find the specified data in the database.");
							}
							else
							{
								processObject(staticObject, bulkOperationclass, "", false);
								bulkOprAppService.update(staticObject);
							}
						}
						else if(DEBulkOperationClass != null)
						{
							staticObject = bulkOperationclass.getClassDiscriminator(valueTable, "");
							if(staticObject == null)
							{
								staticObject = bulkOperationclass.getNewInstance();
							}
							processObject(staticObject, bulkOperationclass, "", false);
							DEObject = processDEObjectAndHooking(valueTable, staticObject);
						}
					}
					else
					{	
						staticObject = bulkOperationclass.getClassDiscriminator(valueTable, "");
						if(staticObject == null)
						{
							staticObject = bulkOperationclass.getNewInstance();
						}
						processObject(staticObject, bulkOperationclass, "", false);
						bulkOprAppService.insert(staticObject);
						if (DEBulkOperationClass != null)
						{
							DEObject = processDEObjectAndHooking(valueTable, staticObject);
						}
					}
					if(staticObject != null)
					{
						staticObjectId = bulkOperationclass.invokeGetIdMethod(staticObject);
						resultIds = bulkOperationclass.getClassName() + "Id: " + staticObjectId;
					}
					if(DEObject != null)
					{
						dynExtObjectId = DEBulkOperationClass.invokeGetIdMethod(DEObject);
						resultIds = resultIds + DEBulkOperationClass.getClassName() + "Id: " + dynExtObjectId;
					}
					dataList.addStatusMessage(currentRowIndex, "Success", resultIds);
					successCount++;
				}
				catch (BulkOperationException exp)
				{
					failureCount++;
					dataList.addStatusMessage(currentRowIndex, "Failure", " " + exp.getMessage());
				}
				catch (Exception exp)
				{
					dataList.addStatusMessage(currentRowIndex, "Failure", " " + exp.getMessage());
					failureCount++;
				}
				if ((currentRowIndex % modValue) == 0)
				{
					insertReportInDatabase(successCount, failureCount,
							JobData.JOB_IN_PROGRESS_STATUS);
				}
			}
			insertReportInDatabase(successCount, failureCount, JobData.JOB_COMPLETED_STATUS);
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey
					.getErrorKey(BulkOperationConstants.COMMON_ISSUES_ERROR_KEY);
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
	}

	/**
	 * @param valueTable
	 * @param staticObject
	 * @throws BulkOperationException
	 * @throws Exception
	 */
	private Object processDEObjectAndHooking(Map<String, String> valueTable, Object staticObject)
			throws BulkOperationException, Exception
	{
		Object DEObject = null;
		DEObject = DEBulkOperationClass.getClassDiscriminator(valueTable, "");
		if (DEObject == null)
		{
			DEObject = DEBulkOperationClass.getNewInstance();
		}
		processObject(DEObject, DEBulkOperationClass, "", false);
		if (DEObject != null)
		{
			bulkOprAppService.insertDEObject(DEObject, staticObject);
			Long dynExtObjectId = DEBulkOperationClass.invokeGetIdMethod(DEObject);
			bulkOprAppService.hookStaticDEObject(staticObject,
					dynExtObjectId, DEBulkOperationClass.getContainerId());
		}
		return DEObject;
	}

	/**
	 * @param i
	 * @throws IOException
	 */
	private void insertReportInDatabase(int recordsProcessed, int failureCount, String statusMessage)
			throws BulkOperationException, IOException
	{
		try
		{
			BulkOperationUtility utility = new BulkOperationUtility();
			String commonFileName = bulkOperationclass.getTemplateName() + jobData.getJobID();
			File file = dataList.createCSVReportFile(commonFileName);
			String[] fileNames = file.getName().split(".csv");
			String zipFilePath = CommonServiceLocator.getInstance().getAppHome()
					+ System.getProperty("file.separator") + fileNames[0];
			File zipFile = utility.createZip(file, zipFilePath);
			long localTimetaken = (System.currentTimeMillis() / 1000) - timetaken;
			Object[] keys = {JobData.LOG_FILE_KEY, JobData.NO_OF_RECORDS_PROCESSED_KEY,
					JobData.NO_OF_FAILED_RECORDS_KEY, JobData.TIME_TAKEN_KEY,
					JobData.NO_OF_TOTAL_RECORDS_KEY, JobData.LOG_FILE_NAME_KEY};
			Object[] values = {zipFile, recordsProcessed, failureCount, localTimetaken,
					dataList.size(), zipFile.getName()};
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
	 * 
	 * @param mainObj
	 * @param migrationClass
	 * @param columnSuffix
	 * @param validate
	 * @throws BulkOperationException
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
	 * 
	 * @param mainObj
	 * @param mainMigrationClass
	 * @param columnSuffix
	 * @param validate
	 * @throws BulkOperationException
	 */
	private void processContainments(Object mainObj, BulkOperationClass mainMigrationClass,
			String columnSuffix, boolean validate) throws BulkOperationException
	{
		try
		{
			Iterator<BulkOperationClass> containmentItert = mainMigrationClass
					.getContainmentAssociationCollection().iterator();
			while (containmentItert.hasNext())
			{
				BulkOperationClass containmentMigrationClass = containmentItert.next();
				String cardinality = containmentMigrationClass.getCardinality();
				if (cardinality != null && cardinality.equals("*") && !cardinality.equals(""))
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
								attributeList)
								|| validate)
						{
							Object containmentObject = containmentMigrationClass
									.getClassDiscriminator(dataList.getValue(currentRowIndex),
											columnSuffix + "#" + i);//getNewInstance();
							if (containmentObject == null)
							{
								if (BulkOperationConstants.JAVA_LANG_STRING_DATATYPE
										.equals(containmentMigrationClass.getClassName()))
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
							if (BulkOperationConstants.JAVA_LANG_STRING_DATATYPE
									.equals(containmentMigrationClass.getClassName()))
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
								containmentMigrationClass
										.invokeSetterMethod(roleName, new Class[]{mainObj
												.getClass()}, containmentObject, mainObj);
							}
						}
					}
					String roleName = containmentMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{Collection.class},
							mainObj, containmentObjectCollection);
				}
				else if (cardinality != null && cardinality.equals("1") && !cardinality.equals(""))
				{
					List<String> attributeList = BulkOperationUtility.getAttributeList(
							containmentMigrationClass, columnSuffix);
					if (dataList.checkIfAtLeastOneColumnHasAValue(currentRowIndex, attributeList)
							|| validate)
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
						processObject(containmentObject, containmentMigrationClass, columnSuffix,
								validate);
						String roleName = containmentMigrationClass.getRoleName();
						mainMigrationClass.invokeSetterMethod(roleName,
								new Class[]{containmentObject.getClass()}, mainObj,
								containmentObject);
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
	private void processAssociations(Object mainObj, BulkOperationClass mainMigrationClass,
			String columnSuffix, boolean validate) throws BulkOperationException
	{
		try
		{
			Iterator<BulkOperationClass> associationItert = mainMigrationClass
					.getReferenceAssociationCollection().iterator();
			while (associationItert.hasNext())
			{
				BulkOperationClass associationMigrationClass = associationItert.next();
				String cardinality = associationMigrationClass.getCardinality();
				if (cardinality != null && cardinality.equals("*") && !cardinality.equals("")
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
								attributeList)
								|| validate)
						{
							Object referenceObject = associationMigrationClass
									.getClassDiscriminator(dataList.getValue(currentRowIndex),
											columnSuffix + "#" + i);
							if (referenceObject == null)
							{
								if (BulkOperationConstants.JAVA_LANG_STRING_DATATYPE
										.equals(associationMigrationClass.getClassName()))
								{
									referenceObject = new StringBuffer();
								}
								else
								{
									referenceObject = associationMigrationClass.getNewInstance();
								}
							}
							processObject(referenceObject, associationMigrationClass, columnSuffix
									+ "#" + i, validate);
							if (BulkOperationConstants.JAVA_LANG_STRING_DATATYPE
									.equals(associationMigrationClass.getClassName()))
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
										new Class[]{mainObj.getClass()}, referenceObject, mainObj);
							}
						}
					}
					String roleName = associationMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{Collection.class},
							mainObj, associationObjectCollection);
				}
				else if (cardinality != null && cardinality.equals("1") && !cardinality.equals(""))
				{
					List<String> attributeList = BulkOperationUtility.getAttributeList(
							associationMigrationClass, columnSuffix);
					if (dataList.checkIfAtLeastOneColumnHasAValue(currentRowIndex, attributeList)
							|| validate)
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
						processObject(associatedObject, associationMigrationClass, columnSuffix,
								validate);
						String roleName = associationMigrationClass.getRoleName();
						mainMigrationClass
								.invokeSetterMethod(roleName, new Class[]{associatedObject
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
			String columnSuffix, boolean validate) throws BulkOperationException
	{
		try
		{
			Iterator<Attribute> attributeItertor = mainMigrationClass.getAttributeCollection()
					.iterator();
			Map<String, String> valueTable = dataList.getValue(currentRowIndex);
			while (attributeItertor.hasNext())
			{
				Attribute attribute = attributeItertor.next();
				if (attribute.getDataType() != null && !"".equals(attribute.getDataType()))
				{
					if (valueTable.get(attribute.getCsvColumnName() + columnSuffix) == null)
					{
						throwExceptionForColumnNameNotFound(mainMigrationClass, validate, attribute);
					}
					else
					{
						if ("java.lang.String".equals(mainMigrationClass.getClassName()))
						{
							if (!Validator.isEmpty(valueTable.get(attribute.getCsvColumnName()
									+ columnSuffix)))
							{
								String csvData = valueTable.get(attribute.getCsvColumnName()
										+ columnSuffix);
								Object attributeValue = attribute.getValueOfDataType(csvData,
										validate);
								((StringBuffer) mainObj).append(attributeValue);
							}
						}
						else if ("java.lang.Long".equals(mainMigrationClass.getClassName())
								|| "java.lang.Double".equals(mainMigrationClass.getClassName())
								|| "java.lang.Integer".equals(mainMigrationClass.getClassName())
								|| "java.lang.Boolean".equals(mainMigrationClass.getClassName())
								|| "java.lang.Float".equals(mainMigrationClass.getClassName()))
						{
							if (!Validator.isEmpty(valueTable.get(attribute.getCsvColumnName()
									+ columnSuffix)))
							{
								String csvData = valueTable.get(attribute.getCsvColumnName()
										+ columnSuffix);
								mainObj = csvData;
							}
						}
						else if (String.valueOf(mainObj.getClass()).contains(
								attribute.getBelongsTo()))
						{
							Class dataTypeClass = Class.forName(attribute.getDataType());
							if (!Validator.isEmpty(valueTable.get(attribute.getCsvColumnName()
									+ columnSuffix)))
							{
								String csvData = valueTable.get(attribute.getCsvColumnName()
										+ columnSuffix);
								Object attributeValue = attribute.getValueOfDataType(csvData,
										validate);
								mainMigrationClass.invokeSetterMethod(attribute.getName(),
										new Class[]{dataTypeClass}, mainObj, attributeValue);
							}
						}//else if ends
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
		if (validate)
		{
			ErrorKey errorkey = ErrorKey
					.getErrorKey("bulk.error.csv.column.name.change.validation");
			throw new BulkOperationException(errorkey, null, attribute.getCsvColumnName() + ":"
					+ attribute.getName() + ":" + mainMigrationClass.getClassName());
		}
		else
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.column.name.change");
			throw new BulkOperationException(errorkey, null, "");
		}
	}
}