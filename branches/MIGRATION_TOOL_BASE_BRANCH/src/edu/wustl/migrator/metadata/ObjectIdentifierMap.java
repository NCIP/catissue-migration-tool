package edu.wustl.migrator.metadata;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;


public class ObjectIdentifierMap
{
	Long id;
	
	public Long getId()
	{
		return id;
	}


	
	public void setId(Long id)
	{
		this.id = id;
	}
	String className; 
	Long oldId;
	Long newId;
	//Object newObj;
	Map<String,LinkedHashSet<ObjectIdentifierMap>> containmentObjectIdentifierMap = new LinkedHashMap<String, LinkedHashSet<ObjectIdentifierMap>>();
	
	public void createOldContainmentObjectIdentifierMap(String roleName,Long oldId,String classname)
	{
		ObjectIdentifierMap containmentObjectidentifierMap = new ObjectIdentifierMap(classname); 
		containmentObjectidentifierMap.setOldId(oldId);
		if(!containmentObjectIdentifierMap.containsKey(roleName))
		{
			//LinkedHashSet<ObjectIdentifierMap> containmentIdSet = new LinkedHashSet<ObjectIdentifierMap>();
			LinkedHashSet<ObjectIdentifierMap> containmentObjectIdentifierSet = new LinkedHashSet<ObjectIdentifierMap>(); 
			containmentObjectIdentifierSet.add(containmentObjectidentifierMap);
			containmentObjectIdentifierMap.put(roleName, containmentObjectIdentifierSet);
		}
		else
		{
			containmentObjectIdentifierMap.get(roleName).add(containmentObjectidentifierMap);
		}
	}
	public void upadteNewIdOfContainmentObjectIdentifierMap(String roleName,Long newId,int counter)
	{
		
		if(containmentObjectIdentifierMap.containsKey(roleName))
		{
			LinkedHashSet<ObjectIdentifierMap> containmentIdSet = containmentObjectIdentifierMap.get(roleName);
			
			LinkedHashSet<ObjectIdentifierMap> containmentObjectIdentifierSet = new LinkedHashSet<ObjectIdentifierMap>(); 
			//containmentObjectIdentifierSet.add(containmentObjectidentifierMap);
			//containmentObjectIdentifierMap.put(roleName, containmentObjectIdentifierSet);
		}
		else
		{
			
		}
	}
/*	public Object getNewObj()
	{
		return newObj;
	}

	
	public void setNewObj(Object newObj)
	{
		this.newObj = newObj;
	}
*/
	
/*	public Object getOldObj()
	{
		return oldObj;
	}

	
	public void setOldObj(Object oldObj)
	{
		this.oldObj = oldObj;
	}
	Object oldObj;*/
	public ObjectIdentifierMap(String className)
	{
		this.className = className;
	}
	
	public String getClassName()
	{
		return className;
	}
	
	public void setClassName(String className)
	{
		this.className = className;
	}
	
	public Long getOldId()
	{
		return oldId;
	}
	
	public void setOldId(Long oldId)
	{
		this.oldId = oldId;
	}
	
	public Long getNewId()
	{
		return newId;
	}
	
	public void setNewId(Long newId)
	{
		this.newId = newId;
	}
	public void setOldId(Object obj) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException
	{
		Long oldId = null;
		String className = obj.getClass().getName();
		Method getIdMethod = obj.getClass().getMethod("getId", null);
		Object id = getIdMethod.invoke(obj, null);
		if (id != null)
		{
			oldId = Long.valueOf(id.toString());
		}
		
		this.oldId = oldId;
		//String query = "insert into table catissue_migration_mapping values ('"+className+"',"+oldId+","+null+")";
	}
	public void setNewId(Object obj) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException
	{
		Long newId = null;
		Method getIdMethod = obj.getClass().getMethod("getId", null);
		Object id = getIdMethod.invoke(obj, null);
		if (id != null)
		{
			newId = Long.valueOf(id.toString());
		}
		this.newId = newId;
	}
}
