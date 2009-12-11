package edu.wustl.bulkoperator;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import edu.wustl.bulkoperator.appservice.MigrationAppService;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.BulkOperationMetaData;
import edu.wustl.bulkoperator.metadata.BulkOperationMetadataUtil;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.bulkoperator.util.MigrationConstants;
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
	 * logger Logger - Generic logger.
	 */
	private static final Logger logger = Logger.getCommonLogger(
			BulkOperator.class);
	/**
	 * 
	 */
    private static Properties migrationInstallProperties = null;    
    /**
     * Main method.
     * @param args
     */
    public static void main(String args[])
	{	
		Long startTime = BulkOperationUtility.getTime();
		try
		{
			/*String jbossHome = "G://jboss-4.2.2.GA/";//args[0];
			String userName = "admin@admin.com";//args[1];//"editSpecimen";
			String password = "Login1234";//args[2];//"E://editSpecimen.csv";
			String operationName = "createParticipantRegistration2";//args[3];//"editSpecimen";
			String csvFileAbsolutePath = "E://createParticipantRegistration2.csv";*///args[4];//"E://editSpecimen.csv";
			
			validate( args );
			String jbossHome = args[0];
			String userName = args[1];
			String password = args[2];
			String operationName = args[3];
			String csvFileAbsolutePath = args[4];
			
			/*migrationInstallProperties = BulkOperationUtility.getMigrationInstallProperties();
			String migrationServiceTypeName = migrationInstallProperties.getProperty(
					MigrationConstants.MIGRATION_SERVICE_TYPE);*/
			/*String userName = migrationInstallProperties.getProperty(
					MigrationConstants.CLIENT_SESSION_USER_NAME);
			String password = migrationInstallProperties.getProperty(
					MigrationConstants.CLIENT_SESSION_PASSWORD);*/
			/*String migrationMetaDataXmlFileName = migrationInstallProperties.getProperty(
					MigrationConstants.MIGRATION_METADATA_XML_FILE_NAME);*/
			System.setProperty("javax.net.ssl.trustStore", jbossHome + "/server/default/conf/chap8.keystore");
			MigrationAppService migrationAppService = getMigrationServiceTypeInstance(
					userName, password);

			BulkOperationMetadataUtil unMarshaller = new BulkOperationMetadataUtil();
			BulkOperationMetaData metadata = unMarshaller.unmarshall(
					MigrationConstants.BULK_OPEARTION_META_DATA_XML_FILE_NAME, "mapping.xml");
			List<String> errorList = ValidateBulkOperationXmlTemplate.
						validateXML(operationName, csvFileAbsolutePath, metadata);
			if(errorList.isEmpty())
			{
				initiateBulkOperationFromCommandLine(operationName, csvFileAbsolutePath,
						migrationAppService, metadata);
			}
			else
			{
				logger.info("------------------------ERROR--------------------------------\n");
				for(String error : errorList)
				{
					logger.info(error);
					logger.info("\n");
				}
				logger.info("------------------------ERROR--------------------------------");
			}
		}
		catch (Exception exp)
		{
			logger.info("------------------------ERROR:--------------------------------\n");
			logger.info(exp.getMessage() + "\n");
			logger.debug(exp.getMessage(), exp);
			logger.info("------------------------ERROR:--------------------------------");
		}
		finally
		{
			Long endTime = BulkOperationUtility.getTime();
			Long totalTime = endTime - startTime ;
			logger.info("time taken = " + totalTime + "seconds");
			if(totalTime > 60)
			{
				logger.info("time taken = " + totalTime/60 + "mins");
			}
		}
	}

	public static boolean configureBulkOperation(boolean flag)
	{
		boolean check = false;
		if(flag)
		{
			check = true;
		}
		return check;
	}

	/**
	 * @param operationName
	 * @param csvFileAbsolutePath
	 * @param migrationAppService
	 * @param metadata
	 * @throws ClassNotFoundException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws BulkOperationException
	 */
	private static void initiateBulkOperationFromCommandLine(String operationName,
			String csvFileAbsolutePath,
			MigrationAppService migrationAppService,
			BulkOperationMetaData metadata) throws ClassNotFoundException,
			SecurityException, NoSuchMethodException, IllegalArgumentException,
			IllegalAccessException, InvocationTargetException,
			BulkOperationException
	{
		Collection<BulkOperationClass> classList = metadata.getBulkOperationClass();
		boolean flag = true;
		if (classList != null)
		{
			Iterator<BulkOperationClass> it = classList.iterator();
			while (it.hasNext())
			{
				BulkOperationClass migration = it.next();
				if(migration.getTemplateName().equals(operationName))
				{
					BulkOperationProcessor migrationProcessor = new BulkOperationProcessor(
							migration, migrationAppService);
					migrationProcessor.startBulkOperation(csvFileAbsolutePath);
					flag = false;
					break;
				}
			}
			if(flag)
			{
				logger.info("\n");
				throw new BulkOperationException("\nThe operation name specified does match " +
						"with any the template name specified in the XML template.");
			}
		}
		else
		{
			logger.info("\n");
			throw new BulkOperationException("Error in BULK OPERATION META DATA XML. Please check the " +
					"XML file created.");
		}
	}
	/**
	 * 
	 * @param operationName
	 * @param csvFileData
	 * @param metadata
	 * @param userName
	 * @return File
	 * @throws ClassNotFoundException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws BulkOperationException
	 */
	public static File initiateBulkOperationFromUI(String operationName,
			List<String[]> csvFileData, BulkOperationMetaData metadata,
			String userName)
				throws ClassNotFoundException,
			SecurityException, NoSuchMethodException, IllegalArgumentException,
			IllegalAccessException, InvocationTargetException,
			BulkOperationException
	{
		File file = null;
		try
		{
			Collection<BulkOperationClass> classList = metadata.getBulkOperationClass();
			boolean flag = true;
			if (classList != null)
			{
				Iterator<BulkOperationClass> it = classList.iterator();
				while (it.hasNext())
				{
					BulkOperationClass migration = it.next();
					if(migration.getTemplateName().equals(operationName))
					{
						MigrationAppService migrationAppService = null;
						BulkOperationProcessor migrationProcessor = new BulkOperationProcessor(
								migration, migrationAppService);
						file = migrationProcessor.startBulkOperationFromUI(csvFileData,
								operationName, userName);
						flag = false;
						break;
					}
				}
				if(flag)
				{
					System.out.println("\n");
					throw new BulkOperationException("bulk.error.incorrect.operation.name");
				}
			}
			else
			{
				System.out.println("\n");
				throw new BulkOperationException("bulk.error.bulk.metadata.xml.file");
			}
		}
		catch (Exception e)
		{
			throw new BulkOperationException(e.getMessage());
		}
		return file;
	}
	/**
	 * Returns the Migration Service type instance
	 * @param migrationServiceType
	 * @param username
	 * @param password
	 * @return
	 * @throws BulkOperationException
	 */
    protected static MigrationAppService getMigrationServiceTypeInstance(
			String username, String password) throws BulkOperationException
	{
		MigrationAppService appService = null;
		Class migrationServiceTypeClass;
		try
		{
			logger.info("migrationServiceTypeName : " +
					MigrationConstants.CA_CORE_MIGRATION_APP_SERVICE);
			migrationServiceTypeClass = Class.forName(
					MigrationConstants.CA_CORE_MIGRATION_APP_SERVICE);
			Class[] constructorParameters = new Class[3];
            constructorParameters[0] = boolean.class;
            constructorParameters[1] = String.class;
            constructorParameters[2] = String.class;
			Constructor constructor = migrationServiceTypeClass.getDeclaredConstructor(constructorParameters);
			appService = (MigrationAppService)constructor.newInstance(true, username, password);
		}
		catch (Exception e)
		{
			throw new BulkOperationException("Invalid User Name or Password.");
		}
		return appService;
	}
    
	/**
	 * @param args - args
	 * @throws Exception - Exception
	 */
	private static void validate(String[] args) throws Exception
	{
		if (args.length == 0)
		{
			logger.debug("Please Specify the jbossHome path.");
			throw new Exception("Please Specify the jbossHome path.");
		}
		if (args.length < 2)
		{
			logger.debug("Please specify the loginName.");
			throw new Exception("Please specify the loginName.");
		}
		if (args.length < 3)
		{
			logger.debug("Please specify the password.");
			throw new Exception("Please specify the password.");
		}
		if (args.length < 4)
		{
			logger.debug("Please specify the operation name.");
			throw new Exception("Please specify the operation name.");
		}
		if (args.length < 5)
		{
			logger.debug("Please specify the csvFileName.");
			throw new Exception("Please specify the csvFileName.");
		}				
	}
}