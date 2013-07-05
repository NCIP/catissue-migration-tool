package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.List;

import edu.wustl.bulkoperator.util.BulkOperationConstants;

public class RecordMapper {
	private String name;
	
	private String className;
	
	private String formName;
	
	private String relName;
	
	private String dataType;
	
	private String columnName;
	
	private List<RecordField> fields = new ArrayList<RecordField>();
	
	private List<RecordMapper> collections = new ArrayList<RecordMapper>();
	
	private List<RecordMapper> associations = new ArrayList<RecordMapper>();
	
	private String formIntegrator;
	
	private List<RecordField> integratorCtxtFields = new ArrayList<RecordField>();
	
	private List<String> identifyingFields = new ArrayList<String>();
	
	public String getType() {
		return (className != null) ? BulkOperationConstants.STATIC : BulkOperationConstants.DYNAMIC;
	}
	
	public boolean isUpdateOperation() {
		boolean isUpdate = false;
		if(identifyingFields.size() > 0) {
			isUpdate = true;
		}
		return isUpdate;
	}

	public RecordField getField(String columnName) {
		RecordField result = null;
		for (RecordField field : fields) {
			if (field.getColumnName().equals(columnName)) {
				result = field;
				break;
			}
		}
		
		return result;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}
	
	public String getFormName() {
		return formName;
	}
	
	public void setFormName(String formName) {
		this.formName = formName;
	}

	public String getRelName() {
		return relName;
	}

	public void setRelName(String relName) {
		this.relName = relName;
	}

	public List<RecordField> getFields() {
		return fields;
	}

	public void setFields(List<RecordField> fields) {
		this.fields = fields;
	}
	
	public void addRecordField(RecordField field) {
		this.fields.add(field);
	}

	public List<RecordMapper> getCollections() {
		return collections;
	}

	public void setCollections(List<RecordMapper> collections) {
		this.collections = collections;
	}

	public void addCollection(RecordMapper collection) {
		this.collections.add(collection);
	}
	
	public List<RecordMapper> getAssociations() {
		return associations;
	}

	public void setAssociations(List<RecordMapper> associations) {
		this.associations = associations;
	}

	public void addAssociation(RecordMapper association) {
		this.associations.add(association);
	}
	
	public String getFormIntegrator() {
		return formIntegrator;
	}

	public void setFormIntegrator(String formIntegrator) {
		this.formIntegrator = formIntegrator;
	}

	public List<RecordField> getIntegratorCtxtFields() {
		return integratorCtxtFields;
	}

	public void setIntegratorCtxtFields(List<RecordField> integratorCtxtFields) {
		this.integratorCtxtFields = integratorCtxtFields;
	}

	public void addIntegratorCtxtField(RecordField integratorCtxtField) {
		this.integratorCtxtFields.add(integratorCtxtField);
	}
	
	public List<String> getIdentifyingFields() {
		return identifyingFields;
	}

	public void setIdentifyingFields(List<String> identifyingFields) {
		this.identifyingFields = identifyingFields;
	}
	
	public void addIdentifyingField(String identifyingCol) {
		this.identifyingFields.add(identifyingCol);
	}
	
	public List<RecordField> getIdFields() {
		List<RecordField> idFields = new ArrayList<RecordField>();

		for(RecordField field : fields) {
			if(identifyingFields.contains(field.getColumnName())) {
				idFields.add(field);
			}
		}
		return idFields;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getDataType() {
			return dataType;
	}
}
