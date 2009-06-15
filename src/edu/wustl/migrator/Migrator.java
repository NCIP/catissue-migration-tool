
package edu.wustl.migrator;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import sun.security.util.ObjectIdentifier;

import edu.wustl.migrator.appservice.CaCoreMigrationAppServiceImpl;
import edu.wustl.migrator.appservice.MigrationAppService;
import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.metadata.MigrationMetadata;
import edu.wustl.migrator.metadata.MigrationMetadataUtil;
import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationProperties;
import edu.wustl.migrator.util.MigrationUtility;
import edu.wustl.migrator.util.PreparedStatementUtil;

public class Migrator
{
    public static String unMigratedObjectFlag = "";
	public static void main(String arg[])
	{
		if(arg != null && arg.length > 0)
		{
			unMigratedObjectFlag = arg[0];
		}
		//unMigratedObjectFlag = "yes";
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
			
			MigrationAppService migrationAppService = new CaCoreMigrationAppServiceImpl(true,
					"admin@admin.com", "Mig_catis@04");

			MigrationMetadataUtil unMarshaller = new MigrationMetadataUtil();
			MigrationMetadata metadata = unMarshaller.unmarshall();

			Collection<MigrationClass> classList = metadata.getMigrationClass();

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

}
