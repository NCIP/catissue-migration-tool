
package edu.wustl.migrator.metadata;

import java.io.FileReader;

import org.exolab.castor.mapping.Mapping;
import org.exolab.castor.xml.Unmarshaller;

public class MigrationMetadataUtil
{
	//static ApplicationService appService = null;

	public  MigrationMetadata unmarshall(String migrationMetaDataXmlFileName)
	{
		MigrationMetadata migrate = null;
		try
		{
			// -- Load a mapping file
			Mapping mapping = new Mapping();
			mapping.loadMapping("mapping.xml");

			Unmarshaller un = new Unmarshaller(MigrationMetadata.class);
			un.setMapping(mapping);

			// -- Read in the migration.xml using the mapping
			FileReader in = new FileReader(migrationMetaDataXmlFileName);
			migrate = (MigrationMetadata) un.unmarshal(in);
			in.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e);
		}
		return migrate;
	}

	
}
