
package edu.wustl.bulkoperator.metadata;

import edu.wustl.bulkoperator.util.BulkOperationException;

public class Attribute
{

	String name;
	String dataType;
	String csvColumnName;
	Boolean updateBasedOn;

	public Boolean getUpdateBasedOn()
	{
		return updateBasedOn;
	}

	public void setUpdateBasedOn(Boolean updateBasedOn)
	{
		this.updateBasedOn = updateBasedOn;
	}

	public String getCsvColumnName()
	{
		return csvColumnName;
	}

	public void setCsvColumnName(String csvColumnName)
	{
		this.csvColumnName = csvColumnName;
	}

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

	public Object getValueOfDataType(String value) throws Exception
	{
		Object valueObject = null;
		try
		{
			valueObject = Class.forName(dataType).getConstructor(String.class).newInstance(value);
		}
		catch(Exception ex)
		{
			throw new Exception("Excpetion in initializing value Of correct DataType", ex);
		}
		return valueObject;
	}

}