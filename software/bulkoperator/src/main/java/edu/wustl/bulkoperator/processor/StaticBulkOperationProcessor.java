package edu.wustl.bulkoperator.processor;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;

import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.RecordField;
import edu.wustl.bulkoperator.metadata.RecordMapper;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.util.logger.Logger;

public class StaticBulkOperationProcessor extends AbstractBulkOperationProcessor {
	private static final Logger logger = Logger.getCommonLogger(StaticBulkOperationProcessor.class);

	public StaticBulkOperationProcessor(SessionDataBean sessionDataBean, BulkOperation bulkOperation) {
		super(sessionDataBean, bulkOperation);
	}

	public Long process(CsvReader csvReader, int rowNum) {
		Object staticObject = null;
		
		try {
			AbstractBulkOperationAppService bulkOpAppSvc = getBulkOperationAppSvc();
			RecordMapper recMapper = bulkOperation.getRecordMapper();
			Map<String, Object> propIdx = getPropIdxMap(recMapper, csvReader.getHeaderRow());
			boolean identifyingFields = bulkOperation.getRecordMapper().getIdentifyingFields().size() > 0 ;
			
			if (identifyingFields && (staticObject = BulkOperationUtility.getObject(recMapper, csvReader, propIdx)) != null) {
				setObjectProps(staticObject, recMapper, csvReader, propIdx);
				bulkOpAppSvc.update(staticObject);
			} else {
				staticObject = getObject(recMapper, csvReader, propIdx);
				bulkOpAppSvc.insert(staticObject);
			}			
		} catch (Exception e) {
			logger.error("Error processing row: " + rowNum, e);
			throw new RuntimeException("Error processing row of input file: " + rowNum, e);
		}
		
		return getId(staticObject);
	}
	
	private Object getObject(RecordMapper recordMapper, CsvReader csvReader, Map<String, Object> propIdx) 
	throws Exception {
		Object object = createObject(recordMapper);
		setObjectProps(object, recordMapper, csvReader, propIdx);
		return object;
	}
	
	private Object createObject(RecordMapper recordMapper) 
	throws Exception {
		String className = recordMapper.getClassName();
		Class<?> klass = Class.forName(className);
		return klass.newInstance();
	}
	
	private boolean setObjectProps(Object object, RecordMapper recordMapper, CsvReader csvReader, Map<String, Object> propIdxs) 
	throws Exception {
		boolean isPropSet = false;
		for (RecordField field : recordMapper.getFields()) {
			String propName = field.getName();
			Integer idx = (Integer)propIdxs.get(propName); 
			if (idx != null) {
				Object value = csvReader.getValue(idx);
				if (field.getDateFormat() != null) {
					value = field.getDate((String)value);
				} 
				if (value != null && !value.toString().isEmpty()) {
					isPropSet = true;
					BeanUtils.setProperty(object, propName, value);
				}
			}	
		}
		
		for (RecordMapper association : recordMapper.getAssociations()) {
			Map<String, Object> assocPropIdxs = (Map<String, Object>)propIdxs.get(association.getName());
			if (assocPropIdxs == null) {
				continue;
			}
			
			Object refObj = BeanUtils.getProperty(object, association.getName());
			if (refObj == null) {
				refObj = createObject(association);
			}
			
			boolean isRefObjPropSet = setObjectProps(refObj, association, csvReader, assocPropIdxs);
			if (isRefObjPropSet) {
				if (association.getRelName() != null) {
					BeanUtils.setProperty(refObj, association.getRelName(), object);
				} 

				BeanUtils.setProperty(object, association.getName(), refObj);
			}
		}
			
		for (RecordMapper collection : recordMapper.getCollections()) {
			List<Map<String, Object>> collectionPropIdxsList = (List<Map<String, Object>>)propIdxs.get(collection.getName());
			if (collectionPropIdxsList == null) {
				continue;
			}
			
			Set<Object> elements = new HashSet<Object>();
			for (Map<String, Object> collectionPropIdxs : collectionPropIdxsList) { 
				Object element = createObject(collection);
				boolean isEltPropSet = setObjectProps(element, collection, csvReader, collectionPropIdxs);
				if (isEltPropSet) {
					elements.add(element);
					if(collection.getRelName() != null || !collection.getRelName().isEmpty()) {
						BeanUtils.setProperty(element, collection.getRelName(), object);
					}
				}
			}
		
		
			Collection oldElements = (Collection)PropertyUtils.getProperty(object, collection.getName());
			if (oldElements != null) {
				oldElements.clear();
				oldElements.addAll(elements);				
			} else {
				oldElements = elements;
			}
			
			BeanUtils.setProperty(object, collection.getName(), oldElements);
		}		
		return isPropSet;
	}
	
	private Long getId(Object object) {
		if (object == null) {
			return null;
		}
		
		Long identifier = null;
		try	{
			identifier = Long.parseLong(BeanUtils.getProperty(object, "id"));
		} catch (Exception e) {
			throw new RuntimeException("Error obtaining identifier of object", e);
		}
		
		return identifier;
	}
}