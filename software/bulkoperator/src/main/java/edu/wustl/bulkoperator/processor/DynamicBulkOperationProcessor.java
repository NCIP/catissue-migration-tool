
package edu.wustl.bulkoperator.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.common.dynamicextensions.domain.nui.Container;
import edu.common.dynamicextensions.domain.nui.Control;
import edu.common.dynamicextensions.domain.nui.MultiSelectControl;
import edu.common.dynamicextensions.domain.nui.SubFormControl;
import edu.common.dynamicextensions.domain.nui.UserContext;
import edu.common.dynamicextensions.napi.ControlValue;
import edu.common.dynamicextensions.napi.FormData;
import edu.common.dynamicextensions.napi.FormDataManager;
import edu.common.dynamicextensions.napi.impl.FormDataManagerImpl;
import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.RecordField;
import edu.wustl.bulkoperator.metadata.RecordMapper;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.dao.util.DAOUtility;

public class DynamicBulkOperationProcessor extends AbstractBulkOperationProcessor {

	private static final Logger logger = Logger
			.getCommonLogger(DynamicBulkOperationProcessor.class);
	
	private Container container;
	
	private UserContext userCtxt;

	public DynamicBulkOperationProcessor(final SessionDataBean sessionDataBean, BulkOperation bulkOperation) {
		super(sessionDataBean, bulkOperation);
		
		try {
			container = Container.getContainer(Long.parseLong(bulkOperation.getRecordMapper().getFormName()));			
			
			userCtxt = new UserContext( ) {				
				@Override
				public String getUserName() {
					return sessionDataBean.getUserName();
				}
				
				@Override
				public Long getUserId() {
					return sessionDataBean.getUserId();
				}
				
				@Override
				public String getIpAddress() {
					return sessionDataBean.getIpAddress();
				}
			};
		} catch (Exception e) {
			throw new RuntimeException("Error obtaining container", e);
		}
	}

	public Long process(CsvReader csvReader, int currentRow) {
		Long recordEntryId = null;

		try {
			AbstractBulkOperationAppService bulkOpAppSvc = getBulkOperationAppSvc();
			RecordMapper recMapper = bulkOperation.getRecordMapper();
			Map<String, Object> propIdx = getPropIdxMap(recMapper, csvReader.getHeaderRow());

			DAOUtility.getInstance().beginTransaction();

			FormData formData = getFormData(recMapper, csvReader, propIdx);
			boolean toBeIntegrated = formData.getRecordId() == null;

			FormDataManager formDataManager = new FormDataManagerImpl();
			recordEntryId = formDataManager.saveOrUpdateFormData(userCtxt, formData);
			
			Map<String, String> formIntegratorMap = getIntegratorCtxt(csvReader, recMapper.getIntegratorCtxtFields());
			if(toBeIntegrated) {
				bulkOpAppSvc.integrateFormDataWithStaticObject(sessionDataBean, container.getId(), recordEntryId, formIntegratorMap);
			}
			
			DAOUtility.getInstance().commitTransaction();
		} catch (Exception e) {
			DAOUtility.getInstance().rollbackTransaction();
			logger.error("Error processing row " + currentRow, e);
			throw new RuntimeException("Error processing row " + currentRow, e);
		} 
		
		return recordEntryId;
	}


	private Map<String, String> getIntegratorCtxt(CsvReader csvReader, List<RecordField> integratorCtxtFields) {
		Map<String, String> integratorCtxt = new HashMap<String, String>();
		
		for(RecordField field : integratorCtxtFields) {
			integratorCtxt.put(field.getName(), csvReader.getValue(field.getColumnName()));
		}
		
		return integratorCtxt;
	}

	private FormData getFormData(RecordMapper recMapper, CsvReader csvReader, Map<String, Object> propIdxs) {
		FormData formData = null;
		
		Long recId = null;
		if (!recMapper.getIdentifyingFields().isEmpty()) {
			String idStr = csvReader.getValue(recMapper.getIdentifyingFields().get(0));
			if (idStr != null && !idStr.trim().isEmpty()) {
				recId = Long.parseLong(idStr.trim());
			}
		}
		
		if(recId != null) {
			FormDataManagerImpl formDataManager = new FormDataManagerImpl();
			formData = formDataManager.getFormData(container, recId);
		} else {
			formData = new FormData(container);
		}
		
		setFormDataProps(formData, recMapper, csvReader, propIdxs);
		return formData;
	}

	private void setFormDataProps(FormData formData, RecordMapper recMapper, CsvReader csvReader, Map<String, Object> propIdxs) {
		Container container = formData.getContainer();
		
		// 
		// For Simple Controls of the container
		//
		for(RecordField field : recMapper.getFields()) {
			String ctrlName = field.getName();
			Integer idx = (Integer)propIdxs.get(ctrlName); 
			Object value = csvReader.getValue(idx);
			Control ctrl = container.getControl(ctrlName);
			
			ControlValue ctrlVal = new ControlValue(ctrl, value);
			formData.addFieldValue(ctrlVal);
		}
		
		for(RecordMapper collection : recMapper.getCollections()) {
			List<Map<String, Object>> collectionPropIdxsList = (List<Map<String, Object>>)propIdxs.get(collection.getName());
			if (collectionPropIdxsList == null) {
				continue;
			}

			Control ctrl = container.getControl(collection.getName());
			ControlValue ctrlVal = null;
			
			if(collection.getColumnName() != null) { // this is multi-select control
				String[] msVals = new String[collectionPropIdxsList.size()];
				int i = 0;
				for(Map<String, Object> collPropIdxs : collectionPropIdxsList){
					Integer column = (Integer) collPropIdxs.get(ctrl.getName());
					msVals[i++] = csvReader.getValue(column);
				}
				
				ctrlVal = new ControlValue(ctrl, msVals);
			} else if (ctrl instanceof SubFormControl) {
				List<FormData> subFormsData = new ArrayList<FormData>();
				Container subContainer = ((SubFormControl) ctrl).getSubContainer();
				
				ControlValue cv = formData.getFieldValue(ctrl.getName());
				List<FormData> sfForms = (List<FormData>) cv.getValue();

				
				//
				// ASSUMPTION :: update of the sub-forms ll be done in the same manner as they are inserted.
				//
				int i = 0;
				for (Map<String, Object> collPropIdxs : collectionPropIdxsList) {
					FormData subFormData = null;
					if (sfForms != null && i < sfForms.size() && formData.getRecordId() != null) {
						subFormData = sfForms.get(i++);
					} 
					else {
						subFormData = new FormData(subContainer);
					}
					setFormDataProps(subFormData, collection, csvReader, collPropIdxs);
					if(isFormDataPresent(subFormData)) {
						subFormsData.add(subFormData);
					} 
				}
				ctrlVal = new ControlValue(ctrl, subFormsData); 
			} 
			
			formData.addFieldValue(ctrlVal);
		}
	}

	private boolean isFormDataPresent(FormData formData) {
		boolean isDataPresent = false;
		
		for (ControlValue ctrlVal : formData.getFieldValues()) {
			Control ctrl = ctrlVal.getControl();
			Object val = ctrlVal.getValue();
			
			if (ctrl instanceof MultiSelectControl && val != null) {
				String[] msVals = (String[]) val;
				for (String msVal : msVals) {
					if (!msVal.isEmpty()) {
						isDataPresent = true;
						break;
					}
				}
			}
			
			// 
			// when each sub-form is added, it is validated against its form-data
			//
			else if (ctrl instanceof SubFormControl) {
				isDataPresent = true;
			} else {
				isDataPresent = !val.toString().isEmpty();
			}
			
			if (isDataPresent) {
				break;
			}
		}
		
		return isDataPresent;
	}
}