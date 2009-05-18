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