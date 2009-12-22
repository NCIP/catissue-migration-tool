
package edu.wustl.bulkoperator;

import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.xml.sax.InputSource;

import edu.wustl.bulkoperator.appservice.MigrationAppService;
import edu.wustl.bulkoperator.jobmanager.JobData;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.BulkOperationMetaData;
import edu.wustl.bulkoperator.metadata.BulkOperationMetadataUtil;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.bulkoperator.util.MigrationConstants;
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
	 * 
	 */
	private static Logger logger = Logger.getCommonLogger(BulkOperator.class);
	BulkOperationMetaData metadata;
	
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
		catch (Exception ex)
		{
			throw new BulkOperationException(ex.getMessage(), ex);
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
		catch (Exception ex)
		{
			throw new BulkOperationException(ex.getMessage(), ex);
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
		System.setProperty("operationName", "editSpecimen");
		System.setProperty("csvFileAbsolutePath", "D:/createSpecimen1.csv");
		System.setProperty("xmlFileAbsolutePath", "D:/createSpecimen.xml");
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

			BulkOperator bulkOperator = new BulkOperator(
					xmlFileAbsolutePath, "mapping.xml");
			bulkOperator.startProcess(operationName, userName, password, "1", dataList,
					MigrationConstants.CA_CORE_MIGRATION_APP_SERVICE, null);
		}
		catch (ApplicationException e)
		{
			logger.info("------------------------ERROR:--------------------------------\n");
			logger.info(e.getMsgValues() + "\n");
			//logger.debug(e.getMsgValues(), e.printStackTrace());
			System.out
					.println("Usage: jbossHome loginName password operationName csvFileAbsolutePath \n");
			System.out.println("------------------------ERROR:--------------------------------\n");
			System.out.println("------------------------ERROR:--------------------------------");
			System.out.println("------------------------ERROR:--------------------------------");
		}
		catch (Exception e)
		{
			System.out.println("------------------------ERROR:--------------------------------\n");
			System.out.println("------------------------ERROR:--------------------------------\n");
			System.out.println(e.getMessage() + "\n\n");
			e.printStackTrace();
			System.out
					.println("Usage: operationName csvFileAbsolutePath userName password jbossHome\n");
			System.out.println("------------------------ERROR:--------------------------------\n");
			System.out.println("------------------------ERROR:--------------------------------");
			System.out.println("------------------------ERROR:--------------------------------");
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
			Iterator<BulkOperationClass> it = classList.iterator();
			if(it.hasNext())
			{
				BulkOperationClass bulkOperationClass = it.next();
				MigrationAppService migrationAppService = MigrationAppService.getInstance(
							appServiceClassName, true, userName, password);
				BulkOperationProcessor bulkOperationProcessor = new BulkOperationProcessor(
						bulkOperationClass, migrationAppService, dataList, jobData);
				bulkOperationProcessor.process();
			}
		}
	}
}