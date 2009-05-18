package edu.wustl.migrator.metadata;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


public class IdMap
{
	String className; 
	Long oldId;
	Long newId;

	public IdMap(String className)
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
