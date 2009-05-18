
package edu.wustl.migrator.appservice;

import java.sql.Connection;
import java.sql.Statement;

import edu.wustl.catissuecore.domain.Container;
import edu.wustl.migrator.dao.DAO;
import edu.wustl.migrator.metadata.IdMap;
import gov.nih.nci.system.applicationservice.ApplicationService;

public class MigrationAppServiceImpl implements MigrationAppService
{

	public void deleteObject(Object obj)
	{
		// TODO Auto-generated method stub

	}

	/**
	 * 
	 * @param idMap
	 */
	private static void insertMapEntries(IdMap idMap)
	{
		Connection con = null;
		DAO dao = new DAO();
		try
		{
			con = dao.establishConnection();
			Statement st = con.createStatement();
			String query = null;
			query = "insert into catissue_migration_mapping (object_classname, old_Id, new_Id) values ('"
					+ idMap.getClassName() + "'," + idMap.getOldId() + "," + idMap.getNewId() + ")";
			st.execute(query);
			st.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			dao.destroyConnection(con);
		}
	}

	public void insertObject(Object obj)
	{
		ApplicationService appService = AppServiceImp.getAppService();
		try
		{
			IdMap idMap = new IdMap(obj.getClass().getName());
			idMap.setOldId(obj);
			Object newObj = appService.createObject(obj);
			idMap.setNewId(newObj);
			insertMapEntries(idMap);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public void updateObject(Object obj)
	{
		// TODO Auto-generated method stub

	}

}
