
package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import edu.wustl.common.beans.SessionDataBean;

public class HookingInformation
{
	private Object staticObject;
	private SessionDataBean sessionDataBean;
	private Long dynamicExtensionObjectId;
	private Long rootContainerId;
	private Collection<Attribute> attributeCollection = new ArrayList<Attribute>();
	private Map<String,Object> dataHookingInformation=new HashMap<String, Object>();
	private String categoryName=null;


	public String getCategoryName()
	{
		return categoryName;
	}


	public void setCategoryName(String categoryName)
	{
		this.categoryName = categoryName;
	}

	public Long getRootContainerId()
	{
		return rootContainerId;
	}

	public void setRootContainerId(Long rootContainerId)
	{
		this.rootContainerId = rootContainerId;
	}


	public Map<String, Object> getDataHookingInformation()
	{
		return dataHookingInformation;
	}


	public void setDataHookingInformation(Map<String, Object> map)
	{
		this.dataHookingInformation = map;
	}

	public HookingInformation()
	{

	}

	public SessionDataBean getSessionDataBean()
	{
		return sessionDataBean;
	}

	public void setSessionDataBean(SessionDataBean sessionDataBean)
	{
		this.sessionDataBean = sessionDataBean;
	}

	public final Long getDynamicExtensionObjectId()
	{
		return dynamicExtensionObjectId;
	}

	public final void setDynamicExtensionObjectId(Long dynamicExtensionObjectId)
	{
		this.dynamicExtensionObjectId = dynamicExtensionObjectId;
	}

	public final Object getStaticObject()
	{
		return staticObject;
	}

	public final void setStaticObject(Object staticObject)
	{
		this.staticObject = staticObject;
	}

	public Collection<Attribute> getAttributeCollection()
	{
		return attributeCollection;
	}

	public void setAttributeCollection(Collection<Attribute> attributeCollection)
	{
		this.attributeCollection = attributeCollection;
	}
}