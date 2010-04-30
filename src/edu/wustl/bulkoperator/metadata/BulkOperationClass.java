
package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.hibernate.proxy.HibernateProxy;

import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.dao.util.HibernateMetaData;

public class BulkOperationClass
{

	/**
	 * 
	 */
	private static Logger logger = Logger.getCommonLogger(BulkOperationClass.class);

	private String className;
	private String relationShipType;
	private String cardinality;
	private String roleName;
	private String parentRoleName;
	private String templateName;
	private Integer maxNoOfRecords;
	private Integer batchSize;
	private Integer containerId;
	private Class klass;
	private Collection<BulkOperationClass> referenceAssociationCollection = new ArrayList<BulkOperationClass>();
	private Collection<BulkOperationClass> containmentAssociationCollection = new ArrayList<BulkOperationClass>();
	private Collection<BulkOperationClass> DEAssociationCollection = new ArrayList<BulkOperationClass>();
	private Collection<Attribute> attributeCollection = new ArrayList<Attribute>();

	public Collection<Attribute> getAttributeCollection()
	{
		return attributeCollection;
	}

	public void setAttributeCollection(Collection<Attribute> attributeCollection)
	{
		this.attributeCollection = attributeCollection;
	}

	public String getTemplateName()
	{
		return templateName;
	}

	public void setTemplateName(String templateName)
	{
		this.templateName = templateName;
	}

	public Integer getContainerId()
	{
		return containerId;
	}

	public void setContainerId(Integer containerId)
	{
		this.containerId = containerId;
	}

	public Collection<BulkOperationClass> getReferenceAssociationCollection()
	{
		return referenceAssociationCollection;
	}

	/**
	 * 
	 * @return
	 */
	public Collection<BulkOperationClass> getDEAssociationCollection()
	{
		return DEAssociationCollection;
	}

	/**
	 * 
	 * @param associationCollection
	 */
	public void setDEAssociationCollection(Collection<BulkOperationClass> associationCollection)
	{
		DEAssociationCollection = associationCollection;
	}

	/**
	 * @return the batchSize
	 */
	public Integer getBatchSize()
	{
		return batchSize;
	}

	/**
	 * @param batchSize the batchSize to set
	 */
	public void setBatchSize(Integer batchSize)
	{
		this.batchSize = batchSize;
	}

	public void setReferenceAssociationCollection(
			Collection<BulkOperationClass> referenceAssociationCollection)
	{
		this.referenceAssociationCollection = referenceAssociationCollection;
	}

	public Collection<BulkOperationClass> getContainmentAssociationCollection()
	{
		return containmentAssociationCollection;
	}

	public void setContainmentAssociationCollection(
			Collection<BulkOperationClass> containmentAssociationCollection)
	{
		this.containmentAssociationCollection = containmentAssociationCollection;
	}

	public String getClassName()
	{
		return className;
	}

	public void setClassName(String className)
	{
		this.className = className;
	}

	public String getRelationShipType()
	{
		return relationShipType;
	}

	public void setRelationShipType(String relationShipType)
	{
		this.relationShipType = relationShipType;
	}

	public String getCardinality()
	{
		return cardinality;
	}

	public void setCardinality(String cardinality)
	{
		this.cardinality = cardinality;
	}

	public String getRoleName()
	{
		return roleName;
	}

	public void setRoleName(String roleName)
	{
		this.roleName = roleName;
	}

	public String getParentRoleName()
	{
		return parentRoleName;
	}

	public void setParentRoleName(String parentRoleName)
	{
		this.parentRoleName = parentRoleName;
	}

	public Integer getMaxNoOfRecords()
	{
		return maxNoOfRecords;
	}

	public void setMaxNoOfRecords(Integer maxNoOfRecords)
	{
		this.maxNoOfRecords = maxNoOfRecords;
	}

	public boolean isUpdateOperation()
	{
		boolean isUpdateOperation = false;
		Collection<Attribute> attributes = getAttributeCollection();
		Iterator<Attribute> attributeItertor = attributes.iterator();
		while (attributeItertor.hasNext())
		{
			Attribute attribute = attributeItertor.next();
			if (attribute.getUpdateBasedOn())
			{
				isUpdateOperation = true;
			}
		}
		return isUpdateOperation;
	}

	public Object getClassDiscriminator(Map<String, String> valueList,
			String columnSuffix) throws BulkOperationException
	{
		Object object = null;
		try
		{
			Collection<Attribute> attributes = getAttributeCollection();
			Iterator<Attribute> attributeItertor = attributes.iterator();
			while (attributeItertor.hasNext())
			{
				Attribute attribute = attributeItertor.next();
				Collection<AttributeDiscriminator> attributeDiscriminatorColl = attribute
						.getDiscriminatorCollection();
				if (attributeDiscriminatorColl != null && !attributeDiscriminatorColl.isEmpty())
				{
					Iterator<AttributeDiscriminator> attributeDiscriminatorItertor = attributeDiscriminatorColl
							.iterator();
					while (attributeDiscriminatorItertor.hasNext())
					{
						AttributeDiscriminator attributeDiscriminator = attributeDiscriminatorItertor
								.next();
						String value = valueList.get(attribute.getCsvColumnName() + columnSuffix);
						if (attributeDiscriminator.getName().equalsIgnoreCase(value))
						{
							String discriminatorValue = attributeDiscriminator.getValue();
							object = Class.forName(discriminatorValue).newInstance();
							break;
						}
					}
					break;
				}
			}
		}
		catch (Exception exp)
		{
			logger.debug("Error in Discriminator Object Instantiation." + exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.discriminator");
			throw new BulkOperationException(errorkey, exp, "");
		}
		return object;
	}

	/**
	 * 
	 * @param bulkOperationclass
	 * @return
	 * @throws BulkOperationException
	 */
	public boolean checkForDEAssociationCollectionTag(BulkOperationClass bulkOperationclass)
		throws BulkOperationException
	{
		boolean isDEObjectPresent = false;
		try
		{
			Collection<BulkOperationClass> DEAssociationCollection = getDEAssociationCollection();
			if(DEAssociationCollection != null &&
					!DEAssociationCollection.isEmpty())
			{
				isDEObjectPresent = true;
			}
		}
		catch (Exception exp)
		{
			logger.debug("Error in Checking For DEAssociationCollection Tag. " + exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.checking.deassociation");
			throw new BulkOperationException(errorkey, exp, "");
		}
		return isDEObjectPresent;
	}

	public Class getClassObject() throws BulkOperationException
	{
		try
		{
			if (klass == null)
			{
				klass = Class.forName(className);
			}
		}
		catch (ClassNotFoundException exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey(BulkOperationConstants.COMMON_ISSUES_ERROR_KEY);
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		return klass;
	}

	public Object getNewInstance() throws BulkOperationException
	{
		Object returnObject = null;
		try
		{
			returnObject = Class.forName(className).newInstance();
		}
		catch (Exception exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey(BulkOperationConstants.COMMON_ISSUES_ERROR_KEY);
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		return returnObject;
	}

	public Object invokeGetterMethod(String roleName, Class[] parameterTypes,
			Object objectOnWhichMethodToInvoke, Object... args) throws BulkOperationException
	{
		Object returnObject = null;
		try
		{
			String functionName = BulkOperationUtility.getGetterFunctionName(roleName);
			returnObject = Class.forName(className).getMethod(functionName, parameterTypes).invoke(
					objectOnWhichMethodToInvoke, args);
			if(returnObject instanceof HibernateProxy)
			{
				returnObject = HibernateMetaData.getProxyObjectImpl(returnObject);
			}
		}
		catch (Exception exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey(BulkOperationConstants.COMMON_ISSUES_ERROR_KEY);
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		return returnObject;
	}

	public void invokeSetterMethod(String roleName, Class[] parameterTypes,
			Object objectOnWhichMethodToInvoke, Object... args) throws BulkOperationException
	{
		String functionName = BulkOperationUtility.getSetterFunctionName(roleName);
		try
		{
			objectOnWhichMethodToInvoke.getClass().getMethod(functionName, parameterTypes).invoke(
					objectOnWhichMethodToInvoke, args);
		}
		catch (NoSuchMethodException e)
		{
			try
			{
				Class.forName(className).getMethod(functionName, parameterTypes[0].getSuperclass())
						.invoke(objectOnWhichMethodToInvoke, args);
			}
			catch (NoSuchMethodException e1)
			{
				try
				{
					Class.forName(className).getMethod(functionName,
							parameterTypes[0].getSuperclass().getSuperclass()).invoke(
							objectOnWhichMethodToInvoke, args);
				}
				catch (Exception e2)
				{
					logger.debug(e2.getMessage(), e2);
					ErrorKey errorkey = ErrorKey.getErrorKey(BulkOperationConstants.COMMON_ISSUES_ERROR_KEY);
					throw new BulkOperationException(errorkey, e, e2.getMessage());
				}
			}
			catch (Exception e4)
			{
				logger.debug(e4.getMessage(), e4);
				ErrorKey errorkey = ErrorKey.getErrorKey(BulkOperationConstants.COMMON_ISSUES_ERROR_KEY);
				throw new BulkOperationException(errorkey, e, e4.getMessage());
			}
		}
		catch (Exception exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey(BulkOperationConstants.COMMON_ISSUES_ERROR_KEY);
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
	}
	
	public Long invokeGetIdMethod(Object objectOnWhichMethodToInvoke) throws BulkOperationException
	{
		Long identifier = null;
		try
		{
			identifier = (Long) Class.forName(className).getMethod("getId", null).invoke(
					objectOnWhichMethodToInvoke, null);
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			exp.printStackTrace();
			throw new BulkOperationException(exp.getMessage(), exp);
		}
		return identifier;
	}
	/*
	public void invokeSetIdMethod(Object objectOnWhichMethodToInvoke, Long identifier)
			throws BulkOperationException
	{
		try
		{
			Class.forName(className).getMethod("setId", Long.class).invoke(
					objectOnWhichMethodToInvoke, identifier);
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			exp.printStackTrace();
			throw new BulkOperationException(exp.getMessage(), exp);
		}
	}*/
}