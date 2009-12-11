package edu.wustl.bulkoperator;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.wustl.bulkoperator.metadata.Attribute;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.BulkOperationMetaData;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.util.logger.Logger;

public class ValidateBulkOperationXmlTemplate
{
	/**
	 * logger Logger - Generic logger.
	 */
	private final static Logger logger = Logger.getCommonLogger(ValidateBulkOperationXmlTemplate.class);
	/**
	 * errorList of ArrayList format containing error messages in String format.
	 */
	private static List<String> errorList = new ArrayList<String>();
	/**
	 * Validate XML.
	 * @param opertionName String.
	 * @param csvFile String.
	 * @param bulkOperationMetaData BulkOperationMetaData.
	 * @throws Exception Exception
	 */
	public static List<String> validateXML(String operationName, String csvFile,
			BulkOperationMetaData bulkOperationMetaData) throws Exception
	{
		try
		{
			List<String[]> csvData = BulkOperationUtility.getCSVTemplateColumnNames(csvFile);
			BulkOperationProcessor bulkOperationProcessor = new BulkOperationProcessor();
			Hashtable<String, String> csvColumnNames =
				bulkOperationProcessor.createHashTable(csvData, 1);
			if(bulkOperationMetaData == null)
			{
				logger.debug("Error in parsing the XML template file.");
				throw new BulkOperationException("Error in parsing the XML template file.");
			}
			else
			{
				validateXmlAndCsv(bulkOperationMetaData, operationName,
						bulkOperationProcessor, csvColumnNames);
			}
		}
		catch(Exception exp)
		{
			logger.debug(exp.getMessage(), exp);
			throw new Exception(exp.getMessage(), exp);
		}
		return errorList;
	}
	/**
	 * Validate Xml And Csv.
	 * @param bulkOperationMetaData BulkOperationMetaData.
	 * @param operationName String.
	 * @param bulkOperationProcessor BulkOperationProcessor.
	 * @param csvColumnNames Map of String, String.
	 * @throws Exception Exception.
	 */
	private static void validateXmlAndCsv(BulkOperationMetaData
		bulkOperationMetaData, String operationName,
		BulkOperationProcessor bulkOperationProcessor,
		Hashtable<String, String> csvColumnNames)
		throws Exception
	{
		Collection<BulkOperationClass> classList = bulkOperationMetaData.getBulkOperationClass();
		if (classList != null)
		{
			Iterator<BulkOperationClass> iterator = classList.iterator();
			while(iterator.hasNext())
			{				
				BulkOperationClass bulkOperationClass = iterator.next();
				if(operationName.equals(bulkOperationClass.getTemplateName()))
				{
					validateBulkOperationMainClass(bulkOperationClass, operationName, csvColumnNames);
					if(errorList.isEmpty())
					{
						validateProcessXML(bulkOperationProcessor,
								bulkOperationClass, csvColumnNames);
					}
				}
			}
		}
	}
	/**
	 * Validate Process XML.
	 * @param bulkOperationProcessor BulkOperationProcessor.
	 * @param bulkOperationClass BulkOperationClass.
	 * @param csvColumnNames Map of String, String.
	 * @throws Exception Exception.
	 */
	private static void validateProcessXML(BulkOperationProcessor
		bulkOperationProcessor, BulkOperationClass bulkOperationClass,
		Hashtable<String, String> csvColumnNames)
		throws Exception
	{
		Object domainObject = bulkOperationClass.getClassObject().
								getConstructor().newInstance(null);
		bulkOperationProcessor.processObject(domainObject,
						bulkOperationClass, null, csvColumnNames);
	}
	/**
	 * Validate Bulk Operation Main Class..
	 * @param bulkOperationClass BulkOperationClass
	 * @param operationName String
	 * @param csvColumnNames Map of String, String.
	 * @throws Exception Exception.
	 */
	private static void validateBulkOperationMainClass(BulkOperationClass
		bulkOperationClass, String operationName, Map<String, String> csvColumnNames)
		throws Exception
	{
		try
		{
			Object classObject = bulkOperationClass.getClassObject();
		}
		catch (NullPointerException exp)
		{
			logger.debug("The keyword 'className' is either missing or incorrectly " +
				"written in the XML for the main class tag.", exp);
			throw new BulkOperationException("The keyword 'className' is either missing " +
				"or incorrectly written in the XML for the main class tag.", exp);
		}
		catch (Exception exp)
		{
			logger.debug("The 'className' value mentioned is incorrect for the main XML tag.", exp);
			throw new BulkOperationException("The 'className' value mentioned is incorrect" +
					" for the main XML tag.", exp);
		}
		try
		{
			validateXMLTagAttributes(bulkOperationClass, operationName);
			validateAssociations(bulkOperationClass, operationName, csvColumnNames);
		}
		catch (Exception exp)
		{
			logger.debug(exp.getMessage(), exp);
			throw new Exception(exp.getMessage(), exp);
		}
	}
	/**
	 * Validate Associations.
	 * @param bulkOperationClass BulkOperationClass.
	 * @param operationName String.
	 * @param csvColumnNames Hash table of String, String.
	 * @throws BulkOperationException BulkOperationException.
	 */
	private static void validateAssociations(BulkOperationClass
		bulkOperationClass, String operationName, Map<String, String> csvColumnNames)
		throws BulkOperationException
	{
		if (bulkOperationClass.getContainmentAssociationCollection() != null
				&& !bulkOperationClass.getContainmentAssociationCollection().isEmpty())
		{
			Collection<BulkOperationClass> containmentClassList = bulkOperationClass
					.getContainmentAssociationCollection();
			validateContainmentReference(operationName, containmentClassList, csvColumnNames);
		}
		if (bulkOperationClass.getReferenceAssociationCollection() != null
				&& !bulkOperationClass.getReferenceAssociationCollection().isEmpty())
		{
			Collection<BulkOperationClass> referenceClassList = bulkOperationClass
					.getReferenceAssociationCollection();
			validateContainmentReference(operationName, referenceClassList, csvColumnNames);
		}
		if (bulkOperationClass.getAttributeCollection() != null
				&& !bulkOperationClass.getAttributeCollection().isEmpty())
		{
			Collection<Attribute> attributesClassList = bulkOperationClass
					.getAttributeCollection();
			validateAttributes(attributesClassList, bulkOperationClass, csvColumnNames);
		}
	}
	/**
	 * Validate Attributes.
	 * @param attributesClassList Collection of Attributes.
	 * @param bulkOperationClass BulkOperationClass.
	 * @param csvColumnNames Hash table of String, String.
	 * @throws BulkOperationException BulkOperationException.
	 */
	private static void validateAttributes(Collection<Attribute>
		attributesClassList, BulkOperationClass bulkOperationClass,
		Map<String, String> csvColumnNames)
		throws BulkOperationException
	{
		Class classObject = bulkOperationClass.getClassObject();
		for(Attribute attribute : attributesClassList)
		{
			try
			{
				Field field = null;
				String attributeName = attribute.getName();
				if(attributeName == null)
				{
					logger.debug("The keyword 'attributeName' is either missing or incorrectly " +
							"written in the XML for "+ bulkOperationClass.getClassName() +
							" class tag.");
					errorList.add("The keyword 'attributeName' is either missing or incorrectly " +
							"written in the XML for "+ bulkOperationClass.getClassName() +
							" class tag.");
				}
				else if("".equals(attributeName.trim()))
				{
					logger.debug("The 'attributeName' value mentioned for "
							+ bulkOperationClass.getClassName() + " is incorrect.");
					errorList.add("The 'attributeName' value mentioned for "
						+ bulkOperationClass.getClassName() + " is incorrect.");
				}
				else
				{
					field = getDeclaredField(classObject, attributeName);
					if(field == null)
					{
						logger.debug("The keyword '"+ attributeName +"' is either missing or incorrectly " +
							"written in the XML for "+ bulkOperationClass.getClassName() +
							" class tag.");
						errorList.add("The keyword 'field' is either missing or incorrectly " +
							"written in the XML for "+ bulkOperationClass.getClassName() +
							" class tag.");
					}
				}
				validateColumnName(bulkOperationClass, attribute, csvColumnNames);
				validateDataType(bulkOperationClass, attribute, field, attributeName);
				validateUpdateBasedOn(bulkOperationClass, attribute);
			}
			catch (Exception exp)
			{
				logger.debug(exp.getMessage(), exp);
				errorList.add(exp.getMessage());
			}
		}
	}
	/**
	 * Validate UpdateBasedOn value.
	 * @param bulkOperationClass BulkOperationClass.
	 * @param attribute Attribute.
	 */
	private static void validateUpdateBasedOn(
			BulkOperationClass bulkOperationClass, Attribute attribute)
	{
		boolean updateBasedOn = attribute.getUpdateBasedOn();
		if(Boolean.toString(updateBasedOn) == null ||
				"".equals(Boolean.toString(updateBasedOn)))
		{
			logger.debug("The keyword 'updateBasedOn' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
			errorList.add("The keyword 'updateBasedOn' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
		}
	}
	/**
	 * Validate Data Type.
	 * @param bulkOperationClass BulkOperationClass.
	 * @param attribute Attribute.
	 * @param field Field.
	 * @param attributeName String.
	 */
	private static void validateDataType(BulkOperationClass bulkOperationClass,
			Attribute attribute, Field field, String attributeName)
	{
		String dataType = attribute.getDataType();
		if(dataType == null)
		{
			logger.debug("The keyword 'dataType' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
			errorList.add("The keyword 'dataType' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
		}
		else if(field != null)
		{
			Class fieldDataType = field.getType();
			if(!fieldDataType.toString().contains(dataType))
			{
				logger.debug("The 'fieldDataType' value mentioned for "
					+ bulkOperationClass.getClassName() + " is incorrect.");
				errorList.add("The 'fieldDataType' value mentioned for "
					+ bulkOperationClass.getClassName() + " is incorrect.");
			}
		}
	}
	/**
	 * Validate Column Name.
	 * @param bulkOperationClass BulkOperationClass.
	 * @param attribute Attribute.
	 * @param csvColumnNames Hash table of String, String.
	 */
	private static void validateColumnName(
			BulkOperationClass bulkOperationClass, Attribute attribute,
			Map<String, String> csvColumnNames)
	{
		String csvColumnName = attribute.getCsvColumnName();
		if(csvColumnName == null)
		{
			logger.debug("The keyword 'csvColumnName' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
			errorList.add("The keyword 'csvColumnName' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
		}
		else if(!csvColumnNames.containsKey(csvColumnName))
		{
			logger.debug("The column name '" + csvColumnName + "' of attribute name '"
				+ attribute.getName() + "' in " + bulkOperationClass.getClassName()
				+ " class tag mismatches with the column name specified in " +
				"the CSV template.");
			errorList.add("The column name '" + csvColumnName + "' of attribute name '"
					+ attribute.getName() + "' in " + bulkOperationClass.getClassName()
					+ " class tag mismatches with the column name specified in " +
					"the CSV template.");
		}
	}
	/**
	 * Get Declared Field.
	 * @param classObject Class.
	 * @param attributeName String.
	 * @return Field Field.
	 */
	private static Field getDeclaredField(Class classObject, String attributeName)
	{
		Field field = null;
		boolean flag = false;
		do
		{
			try
			{
				if(flag)
				{
					classObject = classObject.getSuperclass();
				}
				field = classObject.getDeclaredField(attributeName);
				flag = true;
			}
			catch (Exception exp)
			{
				flag = true;
			}
		}
		while(classObject.getSuperclass() != null && field == null);
		return field;
	}
	/**
	 * Validate Containment and Reference.
	 * @param operationName String.
	 * @param classList Collection of BulkOperationClass.
	 * @param csvColumnNames Hash table of String, String values.
	 * @throws BulkOperationException BulkOperationException.
	 */
	private static void validateContainmentReference(String operationName,
		Collection<BulkOperationClass> classList, Map<String, String> csvColumnNames)
		throws BulkOperationException
	{
		for(BulkOperationClass innerClass : classList)
		{
			try
			{
				Class innerClassObject = innerClass.getClassObject();
				validateXMLTagAttributes(innerClass, operationName);
				validateAssociations(innerClass, operationName, csvColumnNames);
			}
			catch (NullPointerException exp)
			{
				logger.debug("The keyword 'className' is either missing or incorrectly " +
					"written for a XML inner class tag.", exp);
				throw new BulkOperationException("The keyword 'className' is either missing " +
					"or incorrectly written for a XML inner class tag.", exp);
			}
			catch (Exception exp)
			{
				logger.debug("The 'className' value mentioned is incorrect in the XML class tag." ,exp);
				throw new BulkOperationException("The 'className' value mentioned is incorrect " +
					"in the XML class tag.", exp);
			}
		}
	}
	/**
	 * Validate XML Tag Attributes.
	 * @param bulkOperationClass BulkOperationClass.
	 * @param operationName String.
	 */
	private static void validateXMLTagAttributes(
			BulkOperationClass bulkOperationClass, String operationName)
	{
		getRelationShipType(bulkOperationClass);
		String templateName = bulkOperationClass.getTemplateName();
		if(!operationName.equals(templateName.trim()))
		{
			logger.debug("The keyword 'templateName' is either missing or incorrectly "
				+ "written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
			errorList.add("The keyword 'templateName' is either missing or incorrectly "
				+ "written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
		}
		boolean isOneToManyAssociation = bulkOperationClass.getIsOneToManyAssociation();
		if(Boolean.toString(isOneToManyAssociation) == null ||
				"".equals(Boolean.toString(isOneToManyAssociation)))
		{
			logger.debug("The keyword 'isOneToManyAssociation' is either missing or incorrectly "
				+ "written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
			errorList.add("The keyword 'isOneToManyAssociation' is either missing or incorrectly "
				+ "written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
		}
		validateCardinality(bulkOperationClass);
		String roleName = bulkOperationClass.getRoleName();
		if(roleName == null || "".equals(roleName.trim()))
		{
			logger.debug("The keyword 'roleName' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
			errorList.add("The keyword 'roleName' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
		}
	}
	/**
	 * Get RelationShip Type.
	 * @param bulkOperationClass BulkOperationClass.
	 */
	private static void getRelationShipType(
			BulkOperationClass bulkOperationClass)
	{
		String relationShipType = bulkOperationClass.getRelationShipType();
		if(relationShipType == null)
		{
			logger.debug("The keyword 'relationShipType' is either missing or incorrectly " +
					"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
			errorList.add("The keyword 'relationShipType' is either missing or incorrectly " +
					"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
		}
		else if(("".equals(relationShipType.trim())) ||
			(!"containment".equals(relationShipType.trim()) && !"main".equals(
			relationShipType.trim()) && !"association".equals(relationShipType.trim())))
		{
			logger.debug("The 'relationShipType' value mentioned for "
				+ bulkOperationClass.getClassName() + " is incorrect. " +
				"Valid values are 'main', 'assocuation' and 'containment' only.");
			errorList.add("The 'relationShipType' value mentioned for "
				+ bulkOperationClass.getClassName() + " is incorrect. " +
				"Valid values are 'main', 'assocuation' and 'containment' only.");
		}
	}
	/**
	 * Validate Cardinality.
	 * @param bulkOperationClass BulkOperationClass.
	 */
	private static void validateCardinality(
			BulkOperationClass bulkOperationClass)
	{
		String cardinality = bulkOperationClass.getCardinality();
		if(cardinality == null)
		{
			logger.debug("The keyword 'cardinality' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
			errorList.add("The keyword 'cardinality' is either missing or incorrectly " +
				"written in the XML for "+ bulkOperationClass.getClassName() + " class tag.");
		}
		else if(("".equals(cardinality.trim())) ||
			(!"*".equals(cardinality.trim()) && !"1".equals(cardinality.trim())))
		{
			logger.debug("The 'cardinality' value mentioned for "
					+ bulkOperationClass.getClassName() + " is incorrect. " +
					"Valid values are '1' and '*' only.");
			errorList.add("The 'cardinality' value mentioned for "
					+ bulkOperationClass.getClassName() + " is incorrect. " +
					"Valid values are '1' and '*' only.");
		}
	}	
}