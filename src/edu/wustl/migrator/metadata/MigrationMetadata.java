/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.migrator.metadata;

import java.util.ArrayList;
import java.util.Collection;

public class MigrationMetadata {
	
	Collection<MigrationClass> migrationMetadataClass  = new ArrayList<MigrationClass>();
	
	public MigrationMetadata()
	{
		//migrationMetadataClass = new ArrayList<MigrationClass>();
	}
	
	public Collection<MigrationClass> getMigrationClass() {
		return migrationMetadataClass;
	}
}