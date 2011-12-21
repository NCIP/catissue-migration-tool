package edu.wustl.bulkoperator.metadata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.ApplicationProperties;

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

	public Object getValueOfDataType(String value, boolean validate, String csvColumnName, String dataType) throws BulkOperationException
	{
		Object valueObject = null;
		if(!validate && dataType.equals("java.util.Date"))
        {
			SimpleDateFormat sdf = null;
			Date testDate = null;
			try 
			{
				if(value.indexOf(":")>-1)
				{
					String DATE_FORMAT_WITH_TIME = ApplicationProperties.getValue("bulk.date.valid.format.withtime");
					sdf = new SimpleDateFormat(DATE_FORMAT_WITH_TIME);
					testDate = sdf.parse(value);
				}
				else
				{
					String DATE_FORMAT = ApplicationProperties.getValue("bulk.date.valid.format");
					sdf = new SimpleDateFormat(DATE_FORMAT);
					testDate = sdf.parse(value);
				}
				if(!sdf.format(testDate).equals(value))
				{
					ErrorKey errorkey = ErrorKey.getErrorKey("bulk.incorrect.data.error");
					throw new BulkOperationException(errorkey, null, csvColumnName);						
				}
			}
			catch (ParseException parseExp) 
			{
				ErrorKey errorkey = ErrorKey.getErrorKey("bulk.date.format.error");
				throw new BulkOperationException(errorkey, parseExp, csvColumnName);
			}
		}
		try
		{
			if(!validate)
			{
				valueObject = Class.forName(dataType).getConstructor(String.class).newInstance(value);
			}			
		}
		catch (Exception ex)
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.incorrect.data.error");
			throw new BulkOperationException(errorkey, null, csvColumnName);
		}
		return valueObject;
	}
}