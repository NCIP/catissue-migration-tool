/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.bulkoperator;

import org.xml.sax.InputSource;

import edu.wustl.bulkoperator.metadata.BulkOperationMetaData;
import edu.wustl.bulkoperator.metadata.BulkOperationMetadataUtil;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.common.util.logger.LoggerConfig;

public class BulkOperator
{

	/**
	 * logger Logger - Generic logger.
	 */
	static
	{
		LoggerConfig.configureLogger(System.getProperty("user.dir") + "/conf");
	}
	/**
	 * logger.
	 */
	private static final Logger logger = Logger.getCommonLogger(BulkOperator.class);
	/**
	 * metadata.
	 */
	protected transient BulkOperationMetaData metadata;

	/**
	 * @return the metadata
	 */
	public BulkOperationMetaData getMetadata()
	{
		return metadata;
	}

	public BulkOperator(String xmlTemplateFilePath, String mappingFilePath)
			throws BulkOperationException
	{
		try
		{
			BulkOperationMetadataUtil bulkOperationMetadataUtil = new BulkOperationMetadataUtil();
			this.metadata = bulkOperationMetadataUtil.unmarshall(xmlTemplateFilePath,
					mappingFilePath);
		}
		catch (BulkOperationException exp)
		{
			logger.debug(exp.getMessage(), exp);
			throw new BulkOperationException(exp.getErrorKey(), exp, exp.getMsgValues());
		}
	}

	public BulkOperator(InputSource xmlTemplate, InputSource mappingFile)
			throws BulkOperationException
	{
		try
		{
			BulkOperationMetadataUtil bulkOperationMetadataUtil = new BulkOperationMetadataUtil();
			this.metadata = bulkOperationMetadataUtil.unmarshall(xmlTemplate, mappingFile);
		}
		catch (BulkOperationException exp)
		{
			logger.debug(exp.getMessage(), exp);
			throw new BulkOperationException(exp.getErrorKey(), exp, exp.getMsgValues());
		}
	}
}