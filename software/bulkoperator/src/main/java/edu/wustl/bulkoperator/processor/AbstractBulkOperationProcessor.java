package edu.wustl.bulkoperator.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.RecordField;
import edu.wustl.bulkoperator.metadata.RecordMapper;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.beans.SessionDataBean;

public abstract class AbstractBulkOperationProcessor implements IBulkOperationProcessor {

	protected BulkOperation bulkOperation = null;
	
	protected SessionDataBean sessionDataBean = null;
	
	protected String serviceImplementorClassName = null;
	
	public AbstractBulkOperationProcessor(SessionDataBean sessionDataBean, BulkOperation bulkOperation) {
		this.sessionDataBean = sessionDataBean;
		this.bulkOperation = bulkOperation;
		
		try {
			serviceImplementorClassName = BulkOperationUtility.getAppServiceName();
		} catch (BulkOperationException e) {
			throw new RuntimeException("Error obtaining service class", e);
		}
	}
	
	public AbstractBulkOperationAppService getBulkOperationAppSvc() {
		try {
			return AbstractBulkOperationAppService.getInstance(serviceImplementorClassName, sessionDataBean.getUserName());
		} catch (Exception e) {
			throw new RuntimeException("Error instantiating bulk operation app svc", e);
		}
	}
	
	public abstract Long process(CsvReader reader, int rowNum);

	protected Map<String, Object> getPropIdxMap(RecordMapper mapper, List<String> columnNames) {
		Map<String, Object> propIdxMap = new HashMap<String, Object>();
		analyzePropIdx(mapper, 0, columnNames.toArray(new String[0]), propIdxMap);
		return propIdxMap;
	}
	
	protected int analyzePropIdx(RecordMapper mapper, int idx, String[] columnNames, Map<String, Object> attrIdx) {
		boolean found = true;
		while (idx < columnNames.length && found) {
			RecordField field = mapper.getField(columnNames[idx]);
			if (field != null && attrIdx.containsKey(field.getName())) {
				break;
			} else if (field != null) {
				attrIdx.put(field.getName(), idx);
				++idx;
			} else {
				found = false;
				for (RecordMapper assoc : mapper.getAssociations()) {
					if (assoc.getField(columnNames[idx]) != null) {
						Map<String, Object> assocAttrIdx = new HashMap<String, Object>();
						attrIdx.put(assoc.getName(), assocAttrIdx);
						idx = analyzePropIdx(assoc, idx, columnNames, assocAttrIdx);
						found = true;
						break;
					}
				}

				if (found) {
					continue;
				}

				for (RecordMapper collection : mapper.getCollections()) {
					if (collection.getField(columnNames[idx]) != null) {
						found = true;
						List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
						attrIdx.put(collection.getName(), mapList);

						int newIdx = idx;
						do {
							Map<String, Object> collectionMap = new HashMap<String, Object>();
							mapList.add(collectionMap);
							idx = newIdx;
							newIdx = analyzePropIdx(collection, idx, columnNames, collectionMap);
						} while (newIdx != idx);
						
						mapList.remove(mapList.size() - 1);

						break;
					}
					
					// MultiValued Control
					else if (collection.getColumnName() != null && collection.getColumnName().equals(columnNames[idx])) {
						List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
						if(attrIdx.get(collection.getName()) != null) {
							continue;
						}
						found = true;
						attrIdx.put(collection.getName(), mapList);

						do {
							Map<String, Object> collectionMap = new HashMap<String, Object>();
							mapList.add(collectionMap);
							collectionMap.put(collection.getName(), idx);

						} while (columnNames[idx].equals(columnNames[++idx]));
						break;
					}
				}	
				//
				// if found is false, the leaf node may be in the underlying collection or association
				//
				
				if (found) {
					continue;
				}
				
				//
				// 1. Check in the association
				//
				for (RecordMapper assoc : mapper.getAssociations()) {
						Map<String, Object> assocAttrIdx = new HashMap<String, Object>();
						int newIdx = analyzePropIdx(assoc, idx, columnNames, assocAttrIdx);
						if(idx != newIdx) {
							attrIdx.put(assoc.getName(), assocAttrIdx);
							idx = newIdx;
							found = true;
							break;
						}
				}	
				
				if (found) {
					continue;
				}
				
				//
				// 2. Check in the collection
				//
				for (RecordMapper collection : mapper.getCollections()) {
					Map<String, Object> collectionMap = new HashMap<String, Object>();
					int newIdx = analyzePropIdx(collection, idx, columnNames, collectionMap);
					
					if (idx != newIdx) {
						List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
						mapList.add(collectionMap);
						attrIdx.put(collection.getName(), mapList);
						idx = newIdx;
						found = true;
						break;
					}
				}
			}
		}
		return idx;
	}
}
