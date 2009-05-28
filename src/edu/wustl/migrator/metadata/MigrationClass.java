package edu.wustl.migrator.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import edu.wustl.migrator.util.MigrationException;
import edu.wustl.migrator.util.MigrationUtility;


public class MigrationClass{
	
	String className;
	String relationShipType;
	Boolean isToMigrate;
	String cardinality;
	String roleName;
	String sql;
	Class klass;
	
	public String getIsToSetNull()
	{
		return isToSetNull;
	}

	
	public void setIsToSetNull(String isToSetNull)
	{
		this.isToSetNull = isToSetNull;
	}

	String isToSetNull;
	
	public String getSql()
	{
		return sql;
	}

	public void setSql(String sql)
	{
		this.sql = sql;
	}

	Collection<MigrationClass> referenceAssociationCollection = new ArrayList<MigrationClass>();
	
	public Collection<MigrationClass> getReferenceAssociationCollection()
	{
		return referenceAssociationCollection;
	}


	
	public void setReferenceAssociationCollection(
			Collection<MigrationClass> referenceAssociationCollection)
	{
		this.referenceAssociationCollection = referenceAssociationCollection;
	}


	
	public Collection<MigrationClass> getContainmentAssociationCollection()
	{
		return containmentAssociationCollection;
	}


	
	public void setContainmentAssociationCollection(
			Collection<MigrationClass> containmentAssociationCollection)
	{
		this.containmentAssociationCollection = containmentAssociationCollection;
	}
	Collection<MigrationClass> containmentAssociationCollection = new ArrayList<MigrationClass>();
	

	/*public String toString()
	{
		String privatedata = className +"\n";
		privatedata += relationShipType +"\n";
		privatedata += isToMigrate +"\n";
		privatedata += cardinality +"\n";
		privatedata += roleName +"\n";
		
		Iterator<MigrationClass> referenceList = referenceAssociationCollection.iterator();
		while(referenceList.hasNext())
		{
			System.out.println(privatedata);
		}
		
		Iterator<MigrationClass> containmentList = containmentAssociationCollection.iterator();
		while(containmentList.hasNext())
		{
			System.out.println(privatedata);
		}
		
		return "";
	}*/
	
	
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


	
	public Boolean getIsToMigrate()
	{
		return isToMigrate;
	}


	
	public void setIsToMigrate(Boolean isToMigrate)
	{
		this.isToMigrate = isToMigrate;
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
	
	public Class getClassObject() throws MigrationException
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
			throw new MigrationException(e.getMessage(),e);
		}  
		return  klass;
	}
	
	public Object getNewInstance() throws MigrationException
	{
		Object returnObject = null;
		try
		{
			returnObject = Class.forName(className).newInstance();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new MigrationException(e.getMessage(),e);
		}  
		return  returnObject;
	}
	
	public Object invokeGetterMethod(String roleName,Class[]parameterTypes,Object objectOnWhichMethodToInvoke, Object...args) throws MigrationException
	{
		Object returnObject = null;
		try
		{
			String functionName = MigrationUtility.getGetterFunctionName(roleName); 
			returnObject = Class.forName(className).getMethod(functionName, parameterTypes).invoke(objectOnWhichMethodToInvoke, args);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new MigrationException(e.getMessage(),e);
		}
		return returnObject;
	}

	public void invokeSetterMethod(String roleName, Class[]parameterTypes,Object objectOnWhichMethodToInvoke, Object...args) throws MigrationException
	{
		Object returnObject = null;
		try
		{
			String functionName = MigrationUtility.getSetterFunctionName(roleName); 
			Class.forName(className).getMethod(functionName, parameterTypes).invoke(objectOnWhichMethodToInvoke, args);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new MigrationException(e.getMessage(),e);
		}
	}
	
	public Long invokeGetIdMethod(Object objectOnWhichMethodToInvoke) throws MigrationException
	{
		Long id  = null;
		try
		{
			id = (Long)Class.forName(className).getMethod("getId", null).invoke(objectOnWhichMethodToInvoke, null);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new MigrationException(e.getMessage(),e);
		}
		return id;
	}

	public void invokeSetIdMethod(Object objectOnWhichMethodToInvoke, Long id) throws MigrationException
	{
		try
		{
			Class.forName(className).getMethod("setId", Long.class).invoke(objectOnWhichMethodToInvoke, id);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new MigrationException(e.getMessage(),e);
		}
	}
}

