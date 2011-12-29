
package edu.wustl.bulkoperator.processor;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.metadata.Attribute;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.HookingInformation;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.Validator;
import edu.wustl.common.util.logger.Logger;

public abstract class AbstractBulkOperationProcessor
{

	private static final Logger logger = Logger
			.getCommonLogger(AbstractBulkOperationProcessor.class);
	protected BulkOperationClass bulkOperationClass = null;
	protected AppServiceInformationObject serviceInformationObject = null;

	public AbstractBulkOperationProcessor(BulkOperationClass bulkOperationClass,
			AppServiceInformationObject serviceInformationObject)
	{
		this.bulkOperationClass = bulkOperationClass;
		this.serviceInformationObject = serviceInformationObject;
	}

	public final BulkOperationClass getBulkOperationClass()
	{
		return bulkOperationClass;
	}

	protected Object getEntityObject(Map<String, String> csvData) throws BulkOperationException
	{
		Object staticObject = bulkOperationClass.getClassDiscriminator(csvData, "");
		if (staticObject == null)
		{
			staticObject = bulkOperationClass.getNewInstance();
		}
		return staticObject;
	}

	abstract Object processObject(Map<String, String> csvData) throws BulkOperationException;

	/**
	 *
	 * @param mainObj
	 * @param migrationClass
	 * @param columnSuffix
	 * @param validate
	 * @throws BulkOperationException
	 */
	public void processObject(Object mainObj, BulkOperationClass migrationClass,
			Map<String, String> csvData, String columnSuffix, boolean validate, int csvRowNumber)
			throws BulkOperationException
	{
		if (migrationClass.getAttributeCollection() != null
				&& !migrationClass.getAttributeCollection().isEmpty())
		{
			processAttributes(mainObj, migrationClass, csvData, columnSuffix, validate);
		}

		if (migrationClass.getContainmentAssociationCollection() != null
				&& !migrationClass.getContainmentAssociationCollection().isEmpty())
		{
			processContainments(mainObj, migrationClass, csvData, columnSuffix, validate,
					csvRowNumber);
		}

		if (migrationClass.getReferenceAssociationCollection() != null
				&& !migrationClass.getReferenceAssociationCollection().isEmpty())
		{
			processAssociations(mainObj, migrationClass, csvData, columnSuffix, validate,
					csvRowNumber);
		}
	}

	/**
	 *
	 * @param mainObj
	 * @param mainMigrationClass
	 * @param csvData
	 * @param columnSuffix
	 * @param validate
	 * @throws BulkOperationException
	 */
	protected void processContainments(Object mainObj, BulkOperationClass mainMigrationClass,
			Map<String, String> csvData, String columnSuffix, boolean validate, int csvRowNumber)
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
						if (BulkOperationUtility
								.checkIfAtLeastOneColumnHasAValueForInnerContainment(csvRowNumber,
										containmentMigrationClass, columnSuffix, csvData)
								|| validate)
						{
							Object containmentObject = containmentMigrationClass
									.getClassDiscriminator(csvData, columnSuffix + "#" + i);//getNewInstance();
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
							processObject(containmentObject, containmentMigrationClass, csvData,
									columnSuffix + "#" + i, validate, csvRowNumber);
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
					if (BulkOperationUtility.checkIfAtLeastOneColumnHasAValueForInnerContainment(
							csvRowNumber, containmentMigrationClass, columnSuffix, csvData)
							|| validate)
					{
						Object containmentObject = mainMigrationClass.invokeGetterMethod(
								containmentMigrationClass.getRoleName(), null, mainObj, null);
						if (containmentObject == null)
						{
							containmentObject = containmentMigrationClass.getClassDiscriminator(
									csvData, columnSuffix);
						}
						if (containmentObject == null)
						{
							Class klass = containmentMigrationClass.getClassObject();
							Constructor constructor = klass.getConstructor(null);
							containmentObject = constructor.newInstance();
						}
						processObject(containmentObject, containmentMigrationClass, csvData,
								columnSuffix, validate, csvRowNumber);
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
	 * @param csvData
	 * @param columnSuffix
	 * @param validate
	 * @throws BulkOperationException
	 */
	private void processAssociations(Object mainObj, BulkOperationClass mainMigrationClass,
			Map<String, String> csvData, String columnSuffix, boolean validate, int csvRowNumber)
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
						if (BulkOperationUtility
								.checkIfAtLeastOneColumnHasAValueForInnerContainment(csvRowNumber,
										associationMigrationClass, columnSuffix, csvData)
								//								(csvRowNumber,
								//								attributeList, csvData)
								|| validate)
						{
							Object referenceObject = associationMigrationClass
									.getClassDiscriminator(csvData, columnSuffix + "#" + i);
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
							processObject(referenceObject, associationMigrationClass, csvData,
									columnSuffix + "#" + i, validate, csvRowNumber);
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
					if (BulkOperationUtility.checkIfAtLeastOneColumnHasAValueForInnerContainment(
							csvRowNumber, associationMigrationClass, columnSuffix, csvData)
							|| validate)
					{
						Object associatedObject = mainMigrationClass.invokeGetterMethod(
								associationMigrationClass.getRoleName(), null, mainObj, null);
						if (associatedObject == null)
						{
							associatedObject = associationMigrationClass.getClassDiscriminator(
									csvData, columnSuffix);
						}
						if (associatedObject == null)
						{
							Class klass = associationMigrationClass.getClassObject();
							Constructor constructor = klass.getConstructor(null);
							associatedObject = constructor.newInstance();
						}
						processObject(associatedObject, associationMigrationClass, csvData,
								columnSuffix, validate, csvRowNumber);
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
	 * @param csvData2
	 * @param columnSuffix
	 * @param validate
	 * @throws BulkOperationException
	 */
	private void processAttributes(Object mainObj, BulkOperationClass mainMigrationClass,
			Map<String, String> csvData, String columnSuffix, boolean validate)
			throws BulkOperationException
	{
		try
		{
			Iterator<Attribute> attributeItertor = mainMigrationClass.getAttributeCollection()
					.iterator();
			while (attributeItertor.hasNext())
			{
				Attribute attribute = attributeItertor.next();

				if (attribute.getDataType() != null && !"".equals(attribute.getDataType()))
				{
					if (csvData.get(attribute.getCsvColumnName() + columnSuffix) == null)
					{
						BulkOperationUtility.throwExceptionForColumnNameNotFound(
								mainMigrationClass, validate, attribute);
					}
					else
					{
						if ("java.lang.String".equals(mainMigrationClass.getClassName()))
						{
							if (!Validator.isEmpty(csvData.get(attribute.getCsvColumnName()
									+ columnSuffix)))
							{
								String csvDataValue = csvData.get(attribute.getCsvColumnName()
										+ columnSuffix);
								Object attributeValue = attribute.getValueOfDataType(csvDataValue,
										validate, attribute.getCsvColumnName() + columnSuffix,
										attribute.getDataType());
								((StringBuffer) mainObj).append(attributeValue);
							}
						}
						else if ("java.lang.Long".equals(mainMigrationClass.getClassName())
								|| "java.lang.Double".equals(mainMigrationClass.getClassName())
								|| "java.lang.Integer".equals(mainMigrationClass.getClassName())
								|| "java.lang.Boolean".equals(mainMigrationClass.getClassName())
								|| "java.lang.Float".equals(mainMigrationClass.getClassName()))
						{
							if (!Validator.isEmpty(csvData.get(attribute.getCsvColumnName()
									+ columnSuffix)))
							{
								csvData.get(attribute.getCsvColumnName() + columnSuffix);
								mainObj = csvData;
							}
						}
						else if (String.valueOf(mainObj.getClass()).contains(
								attribute.getBelongsTo()))
						{
							Class dataTypeClass = Class.forName(attribute.getDataType());
							setValueToObject(mainObj, mainMigrationClass, csvData, columnSuffix,
									validate, attribute, dataTypeClass);
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

	protected void setValueToObject(Object mainObj, BulkOperationClass mainMigrationClass,
			Map<String, String> csvData, String columnSuffix, boolean validate,
			Attribute attribute, Class dataTypeClass) throws BulkOperationException
	{
		if (!Validator.isEmpty(csvData.get(attribute.getCsvColumnName() + columnSuffix)))
		{
			String csvDataValue = csvData.get(attribute.getCsvColumnName() + columnSuffix);
			Object attributeValue = attribute.getValueOfDataType(csvDataValue, validate, attribute
					.getCsvColumnName()
					+ columnSuffix, attribute.getDataType());
			mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{dataTypeClass},
					mainObj, attributeValue);
		}
	}

	/**
	 *
	 * @param mainMigrationClass
	 * @param validate
	 * @param attribute
	 * @throws BulkOperationException
	 */
	/*protected void throwExceptionForColumnNameNotFound(BulkOperationClass mainMigrationClass, boolean validate,
			Attribute attribute) throws BulkOperationException
	{
		if (validate)
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.column.name.change.validation");
			throw new BulkOperationException(errorkey, null, attribute.getCsvColumnName() + ":" + attribute.getName()
					+ ":" + mainMigrationClass.getClassName());
		}
		else
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.column.name.change");
			throw new BulkOperationException(errorkey, null, "");
		}
	}*/

	protected void getinformationForHookingData(Map<String, String> csvData,
			HookingInformation hookingInformation) throws ClassNotFoundException,
			BulkOperationException
	{
		Iterator<Attribute> attributeItertor = hookingInformation.getAttributeCollection()
				.iterator();
		Map<String, Object> map = new HashMap<String, Object>();
		while (attributeItertor.hasNext())
		{
			Attribute attribute = attributeItertor.next();

			if (!Validator.isEmpty(csvData.get(attribute.getCsvColumnName())))
			{
				String csvDataValue = csvData.get(attribute.getCsvColumnName());
				Object attributeValue = attribute.getValueOfDataType(csvDataValue, false, attribute
						.getCsvColumnName(), attribute.getDataType());
				map.put(attribute.getName(), attributeValue);
			}
		}
		hookingInformation.setDataHookingInformation(map);
	}
}
