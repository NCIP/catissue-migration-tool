
package edu.wustl.migrator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import edu.wustl.catissuecore.domain.Specimen;
import edu.wustl.migrator.appservice.AppServiceImp;
import edu.wustl.migrator.appservice.MigrationAppServiceImpl;
import edu.wustl.migrator.metadata.IdMap;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.metadata.MigrationMetadata;
import edu.wustl.migrator.metadata.MigrationMetadataUtil;

public class Migrator
{

	public static void main(String arg[])
	{
		try
		{
			AppServiceImp.initializeAppService();
			MigrationMetadataUtil unMarshaller = new MigrationMetadataUtil();
			MigrationMetadata metadata = unMarshaller.unmarshall();

			Collection<MigrationClass> classList = metadata.getMigrationClass();

			if (classList != null)
			{
				Iterator<MigrationClass> it = classList.iterator();
				while (it.hasNext())
				{
					MigrationClass migration = it.next();

					List<Long> ids = unMarshaller.migratingDataIDs(migration);

					Processor p = new Processor(migration);
					//p.fetchObjectIdentifier();
					List<Object> list = p.fetchObjects();
					System.out.println("list == " + list.size());
					//list of idMap to be inserted
					List<IdMap> listOfIdMap = new ArrayList<IdMap>();

					if (list != null && !list.isEmpty())
					{
						Iterator<Object> iterator = list.iterator();
						while (iterator.hasNext())
						{
							Object obj = iterator.next();
							new MigrationAppServiceImpl().insertObject(obj);
						}
					}

				}
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
