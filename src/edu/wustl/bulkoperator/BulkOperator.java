package edu.wustl.bulkoperator;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import edu.wustl.bulkoperator.appservice.MigrationAppService;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.BulkOperationMetaData;
import edu.wustl.bulkoperator.metadata.BulkOperationMetadataUtil;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.bulkoperator.util.MigrationConstants;

public abstract class BulkOperator
{	
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
			validate( args );
			String userName = args[0];//"editSpecimen";
			String password = args[1];//"E://editSpecimen.csv";
			String operationName = args[2];//"editSpecimen";
			String csvFileAbsolutePath = args[3];//"E://editSpecimen.csv";
			
			migrationInstallProperties = BulkOperationUtility.getMigrationInstallProperties();
			/*String migrationServiceTypeName = migrationInstallProperties.getProperty(
					MigrationConstants.MIGRATION_SERVICE_TYPE);*/
			/*String userName = migrationInstallProperties.getProperty(
					MigrationConstants.CLIENT_SESSION_USER_NAME);
			String password = migrationInstallProperties.getProperty(
					MigrationConstants.CLIENT_SESSION_PASSWORD);*/
			String migrationMetaDataXmlFileName = migrationInstallProperties.getProperty(
					MigrationConstants.MIGRATION_METADATA_XML_FILE_NAME);
			System.setProperty("javax.net.ssl.trustStore", migrationInstallProperties.getProperty(
					MigrationConstants.JBOSS_HOME) + "/server/default/conf/chap8.keystore");
			MigrationAppService migrationAppService = getMigrationServiceTypeInstance(
					userName, password);

			BulkOperationMetadataUtil unMarshaller = new BulkOperationMetadataUtil();
			BulkOperationMetaData metadata = unMarshaller.unmarshall(migrationMetaDataXmlFileName);

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
					System.out.println("\n");
					throw new BulkOperationException("\nIncorrect OPERATION NAME specified. No such operation " +
							"is allowed in Bulk Operation.");
				}
			}
			else
			{
				System.out.println("\n");
				throw new BulkOperationException("Error in BULK OPERATION META DATA XML. Please check the " +
						"XML file created");
			}
		}
		catch (Exception e)
		{
			System.out.println("------------------------ERROR:--------------------------------\n");
			System.out.println("------------------------ERROR:--------------------------------\n");
			System.out.println(e.getMessage() + "\n\n");
			e.printStackTrace();
			System.out.println("------------------------ERROR:--------------------------------");
			System.out.println("------------------------ERROR:--------------------------------");
		}
		finally
		{
			Long endTime = BulkOperationUtility.getTime();
			Long totalTime = endTime - startTime ;
			System.out.println("time taken = " + totalTime + "seconds");
			if(totalTime > 60)
			{
				System.out.println("time taken = " + totalTime/60 + "mins");
			}
		}
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
			System.out.println("migrationServiceTypeName : " +
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
			throw new Exception("Please Specify the login name.");
		}
		if (args.length < 2)
		{
			throw new Exception("Please specify the password.");
		}
		if (args.length < 3)
		{
			throw new Exception("Please specify the operation name.");
		}
		if (args.length < 4)
		{
			throw new Exception("Please specify the csv file name.");
		}				
	}
}