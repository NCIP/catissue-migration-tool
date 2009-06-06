package edu.wustl.migrator.util;


public class UnMigratedObject
{
	Long id;
	String className;
	Long sandBoxId;
	String message;
	
	public Long getId()
	{
		return id;
	}
	
	public void setId(Long id)
	{
		this.id = id;
	}
	
	public String getClassName()
	{
		return className;
	}
	
	public void setClassName(String className)
	{
		this.className = className;
	}
	
	public Long getSandBoxId()
	{
		return sandBoxId;
	}
	
	public void setSandBoxId(Long sandBoxId)
	{
		this.sandBoxId = sandBoxId;
	}
	
	public String getMessage()
	{
		return message;
	}
	
	public void setMessage(String message)
	{
		this.message = message;
	}
}
