package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.Collection;

import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;


public class BulkOperationClass
{	
	String className;
	String relationShipType;
	Boolean isOneToManyAssociation;
	String cardinality;
	String roleName;
	String templateName;
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


	
	public Boolean getIsOneToManyAssociation()
	{
		return isOneToManyAssociation;
	}


	
	public void setIsOneToManyAssociation(Boolean isOneToManyAssociation)
	{
		this.isOneToManyAssociation = isOneToManyAssociation;
	}


	public String getCardinality() {
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
	
	public Class getClassObject() throws BulkOperationException
	{
		
		try
		{
			if(klass==null)
			{
				klass = Class.forName(className);
			}	
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			throw new BulkOperationException(e.getMessage(),e);
		}  
		return  klass;
	}
	
	public Object getNewInstance() throws BulkOperationException
	{
		Object returnObject = null;
		try
		{
			returnObject = Class.forName(className).newInstance();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new BulkOperationException(e.getMessage(),e);
		}  
		return  returnObject;
	}
	
	public Object invokeGetterMethod(String roleName,Class[]parameterTypes,Object objectOnWhichMethodToInvoke, Object...args) throws BulkOperationException
	{
		Object returnObject = null;
		try
		{
			String functionName = BulkOperationUtility.getGetterFunctionName(roleName); 
			returnObject = Class.forName(className).getMethod(functionName, parameterTypes).invoke(objectOnWhichMethodToInvoke, args);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new BulkOperationException(e.getMessage(),e);
		}
		return returnObject;
	}

	public void invokeSetterMethod(String roleName, Class[]parameterTypes,Object objectOnWhichMethodToInvoke, Object...args) throws BulkOperationException
	{
		Object returnObject = null;
		String functionName = BulkOperationUtility.getSetterFunctionName(roleName); 
		try
		{
			Class.forName(className).getMethod(functionName, parameterTypes).invoke(objectOnWhichMethodToInvoke, args);
		}
		catch(NoSuchMethodException e)
		{
			try
			{
				Class.forName(className).getMethod(functionName, parameterTypes[0].getSuperclass()).invoke(objectOnWhichMethodToInvoke, args);
			}
			catch (NoSuchMethodException e1)
			{
				try
				{
				Class.forName(className).getMethod(functionName, parameterTypes[0].getSuperclass().getSuperclass()).invoke(objectOnWhichMethodToInvoke, args);
				}
				catch(Exception e2)
				{
					e2.printStackTrace();
				}
				
			}
			catch(Exception e4)
			{
				e4.printStackTrace();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new BulkOperationException(e.getMessage(),e);
		}
	}
	
	public Long invokeGetIdMethod(Object objectOnWhichMethodToInvoke) throws BulkOperationException
	{
		Long id  = null;
		try
		{
			id = (Long)Class.forName(className).getMethod("getId", null).invoke(objectOnWhichMethodToInvoke, null);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new BulkOperationException(e.getMessage(),e);
		}
		return id;
	}

	public void invokeSetIdMethod(Object objectOnWhichMethodToInvoke, Long id) throws BulkOperationException
	{
		try
		{
			Class.forName(className).getMethod("setId", Long.class).invoke(objectOnWhichMethodToInvoke, id);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new BulkOperationException(e.getMessage(),e);
		}
	}
}

