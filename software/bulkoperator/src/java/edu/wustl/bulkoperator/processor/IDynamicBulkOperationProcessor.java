/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.bulkoperator.processor;

import java.util.Map;

import edu.wustl.bulkoperator.metadata.HookingInformation;
import edu.wustl.bulkoperator.util.BulkOperationException;

public interface IDynamicBulkOperationProcessor
{

	Object process(Map<String, String> csvData, int csvRowCounter,
			HookingInformation hookingObjectInformation) throws BulkOperationException,
			Exception;
}
