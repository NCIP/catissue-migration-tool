/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.migrator.util;

import java.util.Comparator;


public class SortIds implements Comparator<Long>
{

	public int compare(Long id1, Long id2)
	{
		if (id1 < id2)
		{
			return -1;
		}
		else if (id1 > id2)
		{
			return 1;
		}
		else if (id1 == id2)
		{
			return 0;
		}
		return 0;
	}

}
