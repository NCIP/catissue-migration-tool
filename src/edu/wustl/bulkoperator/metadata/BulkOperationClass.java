/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue-migration-tool/LICENSE.txt for details.
 */

package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

import org.hibernate.proxy.HibernateProxy;

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

	String className;
	String relationShipType;
	String cardinality;
	String roleName;
	String parentRoleName;
	String templateName;
	Integer maxNoOfRecords;
	Integer batchSize;
	Class klass;
	Collection<BulkOperationClass> referenceAssociationCollection = new ArrayList<BulkOperationClass>();
	Collection<BulkOperationClass> containmentAssociationCollection = new ArrayList<BulkOperationClass>();
	Collection<Attribute> attributeCollection = new ArrayList<Attribute>();

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

	public Collection<BulkOperationClass> getReferenceAssociationCollection()
	{
		return referenceAssociationCollection;
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

	public Object getClassDiscriminator(Hashtable<String, String> valueList,
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
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
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
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
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
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
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
					ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
					throw new BulkOperationException(errorkey, e, e2.getMessage());
				}
			}
			catch (Exception e4)
			{
				logger.debug(e4.getMessage(), e4);
				ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
				throw new BulkOperationException(errorkey, e, e4.getMessage());
			}
		}
		catch (Exception exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
	}	
}