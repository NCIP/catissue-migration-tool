
package edu.wustl.bulkoperator.metadata;

import java.io.FileReader;

import org.exolab.castor.mapping.Mapping;
import org.exolab.castor.xml.Unmarshaller;

public class BulkOperationMetadataUtil
{
	//static ApplicationService appService = null;

	public  BulkOperationMetaData unmarshall(String bulkOperationMetaDataXmlFile,
			String xmlMappingFile) throws Exception
	{
		BulkOperationMetaData bulkOperationMetaData = null;
		// -- Load a mapping file
		Mapping mapping = new Mapping();
		mapping.loadMapping(xmlMappingFile);

		Unmarshaller un = new Unmarshaller(BulkOperationMetaData.class);
		un.setMapping(mapping);

		// -- Read in the migration.xml using the mapping
		FileReader fileReader = new FileReader(bulkOperationMetaDataXmlFile);
		bulkOperationMetaData = (BulkOperationMetaData) un.unmarshal(fileReader);
		fileReader.close();
		return bulkOperationMetaData;
	}
}