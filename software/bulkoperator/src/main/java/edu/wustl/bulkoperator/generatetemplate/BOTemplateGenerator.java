package edu.wustl.bulkoperator.generatetemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.common.dynamicextensions.domain.nui.Container;
import edu.common.dynamicextensions.domain.nui.Control;
import edu.common.dynamicextensions.domain.nui.DatePicker;
import edu.common.dynamicextensions.domain.nui.Label;
import edu.common.dynamicextensions.domain.nui.MultiSelectControl;
import edu.common.dynamicextensions.domain.nui.SubFormControl;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.RecordField;
import edu.wustl.bulkoperator.metadata.RecordMapper;
import edu.wustl.bulkoperator.migration.export.BulkOperationSerializer;

public class BOTemplateGenerator {

	private String operationName;
	
	private Container container;
	
	private BulkOperation bulkOperation = new BulkOperation();

	Map<String, String> customFields = new HashMap<String, String>();
    
	private List<String> csvColumnNames = new ArrayList<String>();

	
	public BOTemplateGenerator(String operationName, Container container, Map<String, String> customFields) {
		this.operationName = operationName;
		this.container = container;
		this.customFields = customFields;
	}
	
	
	public BulkOperation generate() throws IOException {
		
		RecordMapper recMapper = getRecordMapper(container);
		recMapper.setIntegratorCtxtFields(getCustomFields());

		bulkOperation.getRecordMapperList().add(recMapper);
		bulkOperation.setBatchSize(10000);
		bulkOperation.setTemplateName(operationName);
		
		return bulkOperation;
	}
	
	public String getTemplateXml() throws IOException {
		BulkOperationSerializer serializer = new BulkOperationSerializer(bulkOperation);
		return(serializer.serialize());
	}

	public String getTemplateCsv() throws IOException {
		StringBuilder csvHeader = new StringBuilder();
		
		for (String colName : csvColumnNames) {
			csvHeader.append(colName).append(",");
		}
		if (csvHeader.length() > 0) {
			csvHeader.delete(csvHeader.length() - 1, csvHeader.length());
		}
		
		return csvHeader.toString();
	}
	
	private RecordMapper getRecordMapper(Container c) {
		RecordMapper recMapper = new RecordMapper();
		recMapper.setFormName(c.getName());
				
		for (Control ctrl : c.getControls()) {
			if (ctrl instanceof Label) {
				continue;
			}
			if (ctrl instanceof SubFormControl) {
				SubFormControl sf = (SubFormControl) ctrl;
				RecordMapper sfRecMapper = getRecordMapper(sf.getSubContainer());
				sfRecMapper.setName(ctrl.getName());
				recMapper.getCollections().add(sfRecMapper);
			} else if (ctrl instanceof MultiSelectControl) {
				RecordMapper msRecMapper = new RecordMapper();
				msRecMapper.setName(ctrl.getName());
				msRecMapper.setColumnName(ctrl.getCaption());
				csvColumnNames.add(ctrl.getCaption());
				recMapper.getCollections().add(msRecMapper);
			} else {
				recMapper.getFields().add(getRecordField(ctrl));
			}
		}
		
		return recMapper;
	}

	private List<RecordField> getCustomFields() {
		List<RecordField> integratorCtxtFields = new ArrayList<RecordField>();
		
		for(Entry<String, String> ctxt : customFields.entrySet()) {
			RecordField recField = new RecordField();
			recField.setColumnName(ctxt.getKey());
			recField.setName(ctxt.getValue());
			integratorCtxtFields.add(recField);
			csvColumnNames.add(ctxt.getKey());
		}
		return integratorCtxtFields;
	}
	
	
	private RecordField getRecordField(Control ctrl) {
		RecordField recField = new RecordField();
		
		String columnName = ctrl.getCaption().replace(" ", "_");
		recField.setName(ctrl.getName());
		String udn=ctrl.getUserDefinedName();
		recField.setColumnName(columnName+"("+udn+")");
		csvColumnNames.add(columnName+"("+udn+")");
		
		if (ctrl instanceof DatePicker) {
			DatePicker dateCtrl = (DatePicker) ctrl;
			recField.setDateFormat(dateCtrl.getFormat());
		}
		
		return recField;
	}
}
