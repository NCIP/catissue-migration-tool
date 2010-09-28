package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.Collection;

public class BulkOperationMetaData {
	
	Collection<BulkOperationClass> bulkOperationMetaDataClassCollection  = new ArrayList<BulkOperationClass>();
	
	public BulkOperationMetaData()
	{

	}

	public Collection<BulkOperationClass> getBulkOperationClass() {
		return bulkOperationMetaDataClassCollection;
	}
}