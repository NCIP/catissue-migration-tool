
package edu.wustl.bulkoperator.metadata;

import java.util.Date;

import edu.wustl.common.beans.SessionDataBean;

public class HookingInformation
{
	private Object staticObject;
	private Long dynamicExtensionObjectId;
	private Long containerId;
	private String clinicalStudyTitle;
	private String clinicalStudyEventLabel;
	private String formLabel;
	private Date encounterDate;
	private SessionDataBean sessionDataBean;





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

	public final Date getEncounterDate()
	{
		return encounterDate;
	}

	public final void setEncounterDate(Date encounterDate)
	{
		this.encounterDate = encounterDate;
	}

	public final Long getContainerId()
	{
		return containerId;
	}

	public final void setContainerId(Long containerId)
	{
		this.containerId = containerId;
	}

	public final Object getStaticObject()
	{
		return staticObject;
	}

	public final void setStaticObject(Object staticObject)
	{
		this.staticObject = staticObject;
	}

	public final String getClinicalStudyTitle()
	{
		return clinicalStudyTitle;
	}

	public final void setClinicalStudyTitle(String clinicalStudyTitle)
	{
		this.clinicalStudyTitle = clinicalStudyTitle;
	}

	public final String getClinicalStudyEventLabel()
	{
		return clinicalStudyEventLabel;
	}

	public final void setClinicalStudyEventLabel(String clinicalStudyEventLabel)
	{
		this.clinicalStudyEventLabel = clinicalStudyEventLabel;
	}

	public final String getFormLabel()
	{
		return formLabel;
	}

	public final void setFormLabel(String formLabel)
	{
		this.formLabel = formLabel;
	}
}