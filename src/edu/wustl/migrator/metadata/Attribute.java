/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.migrator.metadata;

public class Attribute
{

	String name;
	Object valueToSet;
	
	public Object getValueToSet()
	{
		return valueToSet;
	}


	
	public void setValueToSet(Object valueToSet)
	{
		this.valueToSet = valueToSet;
	}

	String isToSetNull;
	String dataType;

	
	

	
	public String getDataType()
	{
		return dataType;
	}

	
	public void setDataType(String dataType)
	{
		this.dataType = dataType;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	

	public String getIsToSetNull()
	{
		return isToSetNull;
	}

	public void setIsToSetNull(String isToSetNull)
	{
		this.isToSetNull = isToSetNull;
	}
}
