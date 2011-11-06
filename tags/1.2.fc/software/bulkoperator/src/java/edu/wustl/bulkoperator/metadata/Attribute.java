
package edu.wustl.bulkoperator.metadata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Locale;

import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.ApplicationProperties;
import edu.wustl.common.util.global.CommonServiceLocator;

public class Attribute
{

	private String name;
	private String dataType;
	private String csvColumnName;
	private Boolean updateBasedOn;
	private String belongsTo;
	private Long id;
	private String format;

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

	public String getFormat()
	{
		return format;
	}

	public void setFormat(String format)
	{
		this.format = format;
	}

	public Object getValueOfDataType(String value, boolean validate, String csvColumnName,
			String dataType) throws BulkOperationException
	{
		Object valueObject = null;
		if (!validate && dataType.equals("java.util.Date")) {
			if (format == null || "".equals(format)) {
					format = ApplicationProperties.getValue("bulk.date.valid.format.withtime");
			}
			try {
				if(format.contains(":") && value!=null && value.contains(":"))
				{
					value=value+" 00:00";
				}
				Locale locale = CommonServiceLocator.getInstance().getDefaultLocale();
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format, locale);
				simpleDateFormat.setLenient(false);
				valueObject = simpleDateFormat.parse(value);
				
			} catch (ParseException parseExp) {
				ErrorKey errorkey = ErrorKey
						.getErrorKey("bulk.date.format.error");
				throw new BulkOperationException(errorkey, parseExp,
						csvColumnName);
			}

		}
		try
		{
			if (!validate && valueObject == null)
			{
				valueObject = Class.forName(dataType).getConstructor(String.class).newInstance(
						value);
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