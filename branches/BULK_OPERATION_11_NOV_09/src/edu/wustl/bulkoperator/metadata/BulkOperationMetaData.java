
package edu.wustl.bulkoperator.metadata;

import java.util.ArrayList;
import java.util.Collection;

public class BulkOperationMetaData
{

	private transient final Collection<BulkOperationClass> bulkOperationMetaDataClassCollection = new ArrayList<BulkOperationClass>();

	public Collection<BulkOperationClass> getBulkOperationClass()
	{
		return bulkOperationMetaDataClassCollection;
	}
}