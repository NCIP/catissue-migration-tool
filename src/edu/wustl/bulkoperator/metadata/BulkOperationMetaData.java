/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue-migration-tool/LICENSE.txt for details.
 */

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