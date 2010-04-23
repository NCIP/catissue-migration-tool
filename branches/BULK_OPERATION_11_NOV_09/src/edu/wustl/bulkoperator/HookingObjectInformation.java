
package edu.wustl.bulkoperator;

public class HookingObjectInformation
{
	private Object staticObject;
	private Long dynamicExtensionObjectId;
	private Long containerId;

	public final Long getDynamicExtensionObjectId()
	{
		return dynamicExtensionObjectId;
	}

	
	public final void setDynamicExtensionObjectId(Long dynamicExtensionObjectId)
	{
		this.dynamicExtensionObjectId = dynamicExtensionObjectId;
	}

	
	public final Long getContainerId()
	{
		return containerId;
	}

	
	public final void setContainerId(Long containerId)
	{
		this.containerId = containerId;
	}

	public HookingObjectInformation(Object staticObject)
	{
		this.staticObject = staticObject;
	}

	public final Object getStaticObject()
	{
		return staticObject;
	}

	public final void setStaticObject(Object staticObject)
	{
		this.staticObject = staticObject;
	}
}
