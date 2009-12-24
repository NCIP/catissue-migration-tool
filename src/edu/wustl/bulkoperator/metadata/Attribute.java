
package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.Collection;

public class Attribute
{

	String name;
	String dataType;
	String csvColumnName;
	Boolean updateBasedOn;
	String belongsTo;
	Collection<AttributeDiscriminator> discriminatorCollection = new ArrayList<AttributeDiscriminator>();

	/**
	 * @return the attributeCollection
	 */
	public Collection<AttributeDiscriminator> getDiscriminatorCollection()
	{
		return discriminatorCollection;
	}

	/**
	 * @param attributeCollection the attributeCollection to set
	 */
	public void setDiscriminatorCollection(
			Collection<AttributeDiscriminator> discriminatorCollection)
	{
		this.discriminatorCollection = discriminatorCollection;
	}

	
	public String getBelongsTo()
	{
		return belongsTo;
	}

	
	public void setBelongsTo(String belongsTo)
	{
		this.belongsTo = belongsTo;
	}

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
		catch (Exception ex)
		{
			throw new Exception("Excpetion in initializing value Of correct DataType", ex);
		}
		return valueObject;
	}

}