
package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.Collection;

public class BulkOperationMetaData
{

	private transient final Collection<BulkOperationClass> bulkOperationMetaDataClassCollection = new ArrayList<BulkOperationClass>();
	private Integer batchSize;
	private String templateName;
	
	public Collection<BulkOperationClass> getBulkOperationClass()
	{
		return bulkOperationMetaDataClassCollection;
	}
	public void addBulkOperationClass(BulkOperationClass bulkOperationClass)
	{
		bulkOperationMetaDataClassCollection.add(bulkOperationClass);	
	}
	
	public Integer getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}
	public String getTemplateName() {
		return templateName;
	}
	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}
}