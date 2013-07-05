package edu.wustl.bulkoperator.actionForm;

import java.io.Serializable;

import org.apache.struts.upload.FormFile;

import edu.wustl.common.actionForm.AbstractActionForm;
import edu.wustl.common.domain.AbstractDomainObject;

/**
 * Bulk operation form from UI.
 * @author sagar_baldwa
 *
 */
public class BulkOperationForm extends AbstractActionForm implements Serializable
{
	private static final long serialVersionUID = -7455581325611509186L;

	private String operationName = "";

	private String dropdownName = "";	

	private FormFile csvFile;

	private FormFile xmlTemplateFile;

	public FormFile getXmlTemplateFile() {
		return xmlTemplateFile;
	}

	public void setXmlTemplateFile(FormFile xmlTemplateFile) {
		this.xmlTemplateFile = xmlTemplateFile;
	}

	public String getOperationName() {
		return operationName;
	}

	public void setOperationName(String operationName) {
		this.operationName = operationName;
	}

	public String getDropdownName() {
		return dropdownName;
	}

	public void setDropdownName(String dropdownName) {
		this.dropdownName = dropdownName;
	}

	public FormFile getCsvFile() {
		return csvFile;
	}

	public void setCsvFile(FormFile file) {
		this.csvFile = file;
	}

	public int getFormId() {
		return 0;
	}

	@Override
	protected void reset() {}

	@Override
	public void setAddNewObjectIdentifier(String arg0, Long arg1) {}

	public void setAllValues(AbstractDomainObject arg0) {}
}