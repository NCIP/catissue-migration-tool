
package edu.wustl.bulkoperator;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.xml.sax.InputSource;

import edu.wustl.bulkoperator.appservice.AbstractBulkOperationAppService;
import edu.wustl.bulkoperator.jobmanager.JobData;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.BulkOperationMetaData;
import edu.wustl.bulkoperator.metadata.BulkOperationMetadataUtil;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.exception.ApplicationException;
import edu.wustl.common.util.global.Validator;
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

	/**
	 * @param args - args
	 * @throws Exception - Exception
	 */
	private static void validateParameters(String operationName, String csvFileAbsolutePath,
			String userName, String password, String jbossHome) throws ApplicationException
	{
		StringBuffer errMsg = new StringBuffer();
		boolean errorFlag = false;
		errMsg.append("Please specify the ");
		if (Validator.isEmpty(jbossHome))
		{
			errorFlag = true;
			errMsg.append("JBoss home directory path,");
		}
		if (Validator.isEmpty(userName))
		{
			errorFlag = true;
			errMsg.append("Application user login name,");
		}
		if (Validator.isEmpty(password))
		{
			errorFlag = true;
			errMsg.append("Password,");
		}
		if (Validator.isEmpty(operationName))
		{
			errorFlag = true;
			errMsg.append("Operation name,");
		}
		if (Validator.isEmpty(csvFileAbsolutePath))
		{
			errorFlag = true;
			errMsg.append("CSV file path,");
		}
		if (errorFlag)
		{
			errMsg.deleteCharAt(errMsg.length() - 1);
			errMsg.append('.');
			throw new ApplicationException(null, null, errMsg.toString());
		}
	}

	/**
	 * Main method.
	 * @param args
	 */
	public static void main(String args[])
	{
		Long startTime = BulkOperationUtility.getTime();
		System.setProperty("operationName", "createSpecimen");
		System.setProperty("csvFileAbsolutePath", "G:/createSpecimen.csv");
		System.setProperty("xmlFileAbsolutePath", "G:/createSpecimen.xml");
		System.setProperty("userName", "admin@admin.com");
		System.setProperty("password", "Login1234");
		System.setProperty("jbossHome", "G:/jboss-4.2.2.GA_8080");
		try
		{
			String operationName = System.getProperty("operationName");
			String csvFileAbsolutePath = System.getProperty("csvFileAbsolutePath");
			String xmlFileAbsolutePath = System.getProperty("xmlFileAbsolutePath");
			String userName = System.getProperty("userName");
			String password = System.getProperty("password");
			String jbossHome = System.getProperty("jbossHome");
			validateParameters(operationName, csvFileAbsolutePath, userName, password, jbossHome);

			System.setProperty("javax.net.ssl.trustStore", jbossHome
					+ "/server/default/conf/chap8.keystore");
			FileInputStream inputStream = new FileInputStream(csvFileAbsolutePath);
			Properties properties = new Properties();
			properties.put("inputStream", inputStream);
			DataList dataList = DataReader.getNewDataReaderInstance(properties).readData();

			BulkOperator bulkOperator = new BulkOperator(xmlFileAbsolutePath, "mapping.xml");
			bulkOperator.startProcess(operationName, userName, password, "1", dataList,
					BulkOperationConstants.CA_CORE_MIGRATION_APP_SERVICE, null);
		}
		catch (ApplicationException appExp)
		{
			logger.info(BulkOperationConstants.ERROR_CONSOLE_FORMAT + 
					BulkOperationConstants.NEW_LINE);
			logger.info(appExp.getMsgValues() + "\n");
			//logger.debug(e.getMsgValues(), e.printStackTrace());
			System.out
					.println("Usage: jbossHome loginName password operationName csvFileAbsolutePath \n");
			System.out.println(BulkOperationConstants.ERROR_CONSOLE_FORMAT + 
					BulkOperationConstants.NEW_LINE);
			System.out.println(BulkOperationConstants.ERROR_CONSOLE_FORMAT);
			System.out.println(BulkOperationConstants.ERROR_CONSOLE_FORMAT);
		}
		catch (Exception exp)
		{
			System.out.println(BulkOperationConstants.ERROR_CONSOLE_FORMAT + 
					BulkOperationConstants.NEW_LINE);
			System.out.println(BulkOperationConstants.ERROR_CONSOLE_FORMAT + 
					BulkOperationConstants.NEW_LINE);
			System.out.println(exp.getMessage() + "\n\n");
			exp.printStackTrace();
			System.out
					.println("Usage: operationName csvFileAbsolutePath userName password jbossHome\n");
			System.out.println(BulkOperationConstants.ERROR_CONSOLE_FORMAT + 
					BulkOperationConstants.NEW_LINE);
			System.out.println(BulkOperationConstants.ERROR_CONSOLE_FORMAT);
			System.out.println(BulkOperationConstants.ERROR_CONSOLE_FORMAT);
		}
		finally
		{
			Long endTime = BulkOperationUtility.getTime();
			Long totalTime = endTime - startTime;
			System.out.println("time taken = " + totalTime + "seconds");
			if (totalTime > 60)
			{
				System.out.println("time taken = " + totalTime / 60 + "mins");
			}
		}
	}

	/**
	 * 
	 * @param operationName
	 * @param userName
	 * @param password
	 * @param dataList
	 * @throws ClassNotFoundException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws BulkOperationException
	 */
	public void startProcess(String operationName, String userName, String password, String userId,
			DataList dataList, String appServiceClassName, JobData jobData) throws Exception
	{
		Collection<BulkOperationClass> classList = metadata.getBulkOperationClass();
		if (classList != null)
		{
			Iterator<BulkOperationClass> iterator = classList.iterator();
			if (iterator.hasNext())
			{
				BulkOperationClass bulkOperationClass = iterator.next();
				AbstractBulkOperationAppService bulkOprAppService = AbstractBulkOperationAppService.getInstance(
						appServiceClassName, true, userName, password);
				BulkOperationProcessor bulkOperationProcessor = new BulkOperationProcessor(
						bulkOperationClass, bulkOprAppService, dataList, jobData);
				bulkOperationProcessor.process();
			}
		}
	}
}