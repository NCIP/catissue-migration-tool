
package edu.wustl.bulkoperator.metadata;

import java.io.FileReader;

import org.exolab.castor.mapping.Mapping;
import org.exolab.castor.xml.Unmarshaller;

public class BulkOperationMetadataUtil
{
	//static ApplicationService appService = null;

	public  BulkOperationMetaData unmarshall(String migrationMetaDataXmlFileName)
	{
		BulkOperationMetaData bulkOperationMetaData = null;
		try
		{
			// -- Load a mapping file
			Mapping mapping = new Mapping();
			mapping.loadMapping("mapping.xml");

			Unmarshaller un = new Unmarshaller(BulkOperationMetaData.class);
			un.setMapping(mapping);

			// -- Read in the migration.xml using the mapping
			FileReader fileReader = new FileReader(migrationMetaDataXmlFileName);
			bulkOperationMetaData = (BulkOperationMetaData) un.unmarshal(fileReader);
			fileReader.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e);
		}
		return bulkOperationMetaData;
	}

	
}
