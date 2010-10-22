
package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.Collection;

import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.exception.ErrorKey;

public class Attribute
{

	private String name;
	private String dataType;
	private String csvColumnName;
	private Boolean updateBasedOn;
	private String belongsTo;
	private Long id;
	private Collection<AttributeDiscriminator> discriminatorCollection = new ArrayList<AttributeDiscriminator>();

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
	
	public Long getId()
	{
		return id;
	}
	
	public void setId(Long id)
	{
		this.id = id;
	}
	public Object getValueOfDataType(String value, boolean validate) throws BulkOperationException
	{
		Object valueObject = null;
		try
		{
			if(!validate)
			{
				valueObject = Class.forName(dataType).getConstructor(String.class).newInstance(value);
			}			
		}
		catch (Exception exp)
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.incorrect.data.error");
			throw new BulkOperationException(errorkey, exp, value);
		}
		return valueObject;
	}
}