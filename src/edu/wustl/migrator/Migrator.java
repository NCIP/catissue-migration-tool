
package edu.wustl.migrator;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import edu.wustl.migrator.appservice.CaCoreMigrationAppServiceImpl;
import edu.wustl.migrator.appservice.MigrationAppService;
import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.metadata.MigrationMetadata;
import edu.wustl.migrator.metadata.MigrationMetadataUtil;
import gov.nih.nci.system.applicationservice.ApplicationException;
import gov.nih.nci.system.applicationservice.ApplicationServiceProvider;
import gov.nih.nci.system.comm.client.ClientSession;

public class Migrator
{

	public static void main(String arg[])
	{
		try
		{
			SandBoxDao.init();
			//SandBoxDao.initializeIdMap();

			MigrationAppService migrationAppService = new CaCoreMigrationAppServiceImpl(true,
					"admin@admin.com", "login123");
			
			MigrationMetadataUtil unMarshaller = new MigrationMetadataUtil();
			MigrationMetadata metadata = unMarshaller.unmarshall();

			Collection<MigrationClass> classList = metadata.getMigrationClass();

			if (classList != null)
			{
				Iterator<MigrationClass> it = classList.iterator();
				while (it.hasNext())
				{
					MigrationClass migration = it.next();
					MigrationProcessor migrationProcessor = new MigrationProcessor(migration,migrationAppService);

					List<Object> list = migrationProcessor.fetchObjects();

					System.out.println("list == " + list.size());

					if (list != null && !list.isEmpty())
					{
						Iterator<Object> iterator = list.iterator();
						while (iterator.hasNext())
						{
							Object obj = iterator.next();
							migrationAppService.insert(obj, migration);
						}
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
			SandBoxDao.closeSession();
		}
	}

}
