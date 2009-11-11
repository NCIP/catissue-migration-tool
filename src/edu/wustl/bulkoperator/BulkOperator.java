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
		migrationInstallProperties = BulkOperationUtility.getMigrationInstallProperties();
		Long startTime = BulkOperationUtility.getTime();
		try
		{
			String migrationServiceTypeName = migrationInstallProperties.getProperty(
					MigrationConstants.MIGRATION_SERVICE_TYPE);
			System.out.println("migrationServiceTypeName : " + migrationServiceTypeName);
			String userName = migrationInstallProperties.getProperty(
					MigrationConstants.CLIENT_SESSION_USER_NAME);
			String password = migrationInstallProperties.getProperty(
					MigrationConstants.CLIENT_SESSION_PASSWORD);
			String migrationMetaDataXmlFileName = migrationInstallProperties.getProperty(
					MigrationConstants.MIGRATION_METADATA_XML_FILE_NAME);
			System.setProperty("javax.net.ssl.trustStore", migrationInstallProperties.getProperty(
					MigrationConstants.JBOSS_HOME) + "/server/default/conf/chap8.keystore");
			MigrationAppService migrationAppService = getMigrationServiceTypeInstance(
					migrationServiceTypeName, userName, password);
			
			BulkOperationMetadataUtil unMarshaller = new BulkOperationMetadataUtil();
			BulkOperationMetaData metadata = unMarshaller.unmarshall(migrationMetaDataXmlFileName);

			Collection<BulkOperationClass> classList = metadata.getBulkOperationClass();
			if (classList != null)
			{
				Iterator<BulkOperationClass> it = classList.iterator();
				while (it.hasNext())
				{
					BulkOperationClass migration = it.next();
					if("createParticipant".equals(migration.getTemplateName()))
					{
						BulkOperationProcessor migrationProcessor = new BulkOperationProcessor(
								migration, migrationAppService);
						migrationProcessor.startBulkOperation();
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
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
    protected static MigrationAppService getMigrationServiceTypeInstance(String migrationServiceType,
			String username, String password) throws BulkOperationException
	{
		MigrationAppService appService = null;
		Class migrationServiceTypeClass;
		try
		{
			migrationServiceTypeClass = Class.forName(migrationServiceType);
			Class[] constructorParameters = new Class[3];
            constructorParameters[0] = boolean.class;
            constructorParameters[1] = String.class;
            constructorParameters[2] = String.class;
			Constructor constructor = migrationServiceTypeClass.getDeclaredConstructor(constructorParameters);
			appService = (MigrationAppService)constructor.newInstance(true, username, password);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new BulkOperationException();
		}
		return appService;
	}
}