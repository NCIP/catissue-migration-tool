
package edu.wustl.bulkoperator.metadata;

import java.io.FileReader;
import java.io.IOException;

import org.exolab.castor.mapping.Mapping;
import org.exolab.castor.mapping.MappingException;
import org.exolab.castor.xml.MarshalException;
import org.exolab.castor.xml.Unmarshaller;
import org.exolab.castor.xml.ValidationException;
import org.xml.sax.InputSource;

import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.logger.Logger;

public class BulkOperationMetadataUtil
{
	/**
	 * logger.
	 */
	private transient final Logger logger = Logger.getCommonLogger(BulkOperationMetadataUtil.class);

	public BulkOperationMetaData unmarshall(String bulkOperationMetaDataXmlFile,
			String xmlMappingFile) throws BulkOperationException
	{
		BulkOperationMetaData bulkOperationMetaData = null;
		try
		{			
			// -- Load a mapping file
			Mapping mapping = new Mapping();
			mapping.loadMapping(xmlMappingFile);
			
			Unmarshaller un = new Unmarshaller(BulkOperationMetaData.class);
			un.setMapping(mapping);
	
			// -- Read in the migration.xml using the mapping
			FileReader fileReader = new FileReader(bulkOperationMetaDataXmlFile);
			bulkOperationMetaData = (BulkOperationMetaData) un.unmarshal(fileReader);
			fileReader.close();
		}
		catch (MarshalException exp)
		{
			logger.debug(exp.getMessage(), exp);
			logger.info(exp.getMessage());
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		catch (ValidationException exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		catch (IOException exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		catch (MappingException exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		catch (Exception exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		return bulkOperationMetaData;
	}
	
	public BulkOperationMetaData unmarshall(InputSource bulkOperationMetaDataXml,
			InputSource xmlMappingFile) throws BulkOperationException
		{BulkOperationMetaData bulkOperationMetaData = null;
		try
		{
			// -- Load a mapping file
			Mapping mapping = new Mapping();
			mapping.loadMapping(xmlMappingFile);
			
			Unmarshaller un = new Unmarshaller(BulkOperationMetaData.class);
			un.setMapping(mapping);
	
			// -- Read in the migration.xml using the mapping
			
			bulkOperationMetaData = (BulkOperationMetaData) un.unmarshal(bulkOperationMetaDataXml);
		}
		catch (MarshalException exp)
		{
			logger.debug(exp.getMessage(), exp);
			logger.info(exp.getMessage());
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		catch (ValidationException exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		catch (MappingException exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		catch (Exception exp)
		{
			logger.debug(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}	
		return bulkOperationMetaData;
	}
}