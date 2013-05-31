/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.migrator.util;


public class UnMigratedObject
{
	Long id;
	String className;
	Long sandBoxId;
	String message;
	String stackTrace;
	
	
	public String getStackTrace()
	{
		return stackTrace;
	}

	
	public void setStackTrace(String stackTrace)
	{
		this.stackTrace = stackTrace;
	}

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
