package edu.wustl.bulkoperator.migration;


public class AttributeDiscriminator
{
	private String name;
	private String value;
	
	/**
	 * @return the discriminatorName
	 */
	public String getName()
	{
		return name;
	}
	
	/**
	 * @param discriminatorName the discriminatorName to set
	 */
	public void setName(String name)
	{
		this.name = name;
	}
	
	/**
	 * @return the discriminatorValue
	 */
	public String getValue()
	{
		return value;
	}
	
	/**
	 * @param discriminatorValue the discriminatorValue to set
	 */
	public void setValue(String value)
	{
		this.value = value;
	}
}
