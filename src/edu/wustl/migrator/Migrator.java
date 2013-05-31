/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.migrator;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.logging.Logger;

import edu.wustl.migrator.appservice.MigrationAppService;
import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.metadata.MigrationMetadata;
import edu.wustl.migrator.metadata.MigrationMetadataUtil;
import edu.wustl.migrator.util.MigrationConstants;
import edu.wustl.migrator.util.MigrationException;
import edu.wustl.migrator.util.MigrationUtility;
import edu.wustl.migrator.util.MigratorThread;
import edu.wustl.migrator.util.PreparedStatementUtil;

public abstract class Migrator
{	
    public static String unMigratedObjectFlag = "";
    private static Properties migrationInstallProperties = null;
    
    /**
     * 
     * @param args
     */
    public static void main(String args[])
	{
		Logger logger = Logger.getLogger("");
		
		migrationInstallProperties = MigrationUtility.getMigrationInstallProperties();
		//System.out.println(migrationInstallProperties + "migrationInstallProperties");
		if(args != null && args.length > 0)
		{
			unMigratedObjectFlag = args[0];
		}
		Long startTime = MigrationUtility.getTime();
		try
		{
			SandBoxDao.init();
			/*ObjectIdentifierMap o1 = new ObjectIdentifierMap("edu.wustl.catissuecore.domain.StorageType");
			o1.setOldId(new Long(1));
			o1.setNewId(new Long(1));
			SandBoxDao.saveObject(o1);
			ObjectIdentifierMap o2 = new ObjectIdentifierMap("edu.wustl.catissuecore.domain.StorageType");
			o2.setOldId(new Long(3));
			o2.setNewId(new Long(3));
			SandBoxDao.saveObject(o2);
			SandBoxDao.initializeIdMap();*/
			
			String migrationServiceTypeName = migrationInstallProperties.getProperty(MigrationConstants.MIGRATION_SERVICE_TYPE);
			System.out.println("migrationServiceTypeName : " + migrationServiceTypeName);
			String userName = migrationInstallProperties.getProperty(MigrationConstants.CLIENT_SESSION_USER_NAME);
			String password = migrationInstallProperties.getProperty(MigrationConstants.CLIENT_SESSION_PASSWORD);
			String migrationMetaDataXmlFileName = migrationInstallProperties.
									getProperty(MigrationConstants.MIGRATION_METADATA_XML_FILE_NAME);
			System.setProperty("javax.net.ssl.trustStore", migrationInstallProperties.
					getProperty(MigrationConstants.JBOSS_HOME) + "/server/default/conf/chap8.keystore");
			MigrationAppService migrationAppService = getMigrationServiceTypeInstance(migrationServiceTypeName, userName, password);
			
//			MigrationAppService migrationAppService = new CaCoreMigrationAppServiceImpl(true,
//					migrationInstallProperties.getProperty(MigrationConstants.CLIENT_SESSION_USER_NAME),
//					migrationInstallProperties.getProperty(MigrationConstants.CLIENT_SESSION_PASSWORD));

			/*MigrationAppService migrationAppService = new CaTissueThickClientService(true,
					migrationInstallProperties.getProperty(MigrationConstants.CLIENT_SESSION_USER_NAME),
					migrationInstallProperties.getProperty(MigrationConstants.CLIENT_SESSION_PASSWORD));*/

			
			MigrationMetadataUtil unMarshaller = new MigrationMetadataUtil();
			MigrationMetadata metadata = unMarshaller.unmarshall(migrationMetaDataXmlFileName);

			Collection<MigrationClass> classList = metadata.getMigrationClass();
			
			MigratorThread migratorThread = new MigratorThread();
			migratorThread.start();
			
			if (classList != null)
			{
				Iterator<MigrationClass> it = classList.iterator();
				while (it.hasNext())
				{
					MigrationClass migration = it.next();
					MigrationProcessor migrationProcessor = new MigrationProcessor(migration,
							migrationAppService);

					migrationProcessor.fetchObjects();
				}
			}
			migratorThread.interrupt();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			PreparedStatementUtil.closePreparedStatements();
			SandBoxDao.closeInsertionSession();
			Long endTime = MigrationUtility.getTime();
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
	 * @throws MigrationException
	 */
    protected static MigrationAppService getMigrationServiceTypeInstance(String migrationServiceType,
			String username, String password) throws MigrationException
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
			throw new MigrationException();
		}
		return appService;
	}
}