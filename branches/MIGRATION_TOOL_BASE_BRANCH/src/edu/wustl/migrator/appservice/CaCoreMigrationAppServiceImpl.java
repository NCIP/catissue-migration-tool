
package edu.wustl.migrator.appservice;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Statement;

import edu.wustl.catissuecore.domain.Container;
import edu.wustl.migrator.MigrationObjectStatusHandler;
import edu.wustl.migrator.dao.DAO;
import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationException;
import gov.nih.nci.system.applicationservice.ApplicationException;
import gov.nih.nci.system.applicationservice.ApplicationService;
import gov.nih.nci.system.applicationservice.ApplicationServiceProvider;
import gov.nih.nci.system.comm.client.ClientSession;

public class CaCoreMigrationAppServiceImpl extends MigrationAppService
{

	ApplicationService appService = null;
	ClientSession clientSession = null;
	
	public CaCoreMigrationAppServiceImpl(boolean isAuthenticationRequired,String userName,String password) throws MigrationException
	{
		super(isAuthenticationRequired, userName, password);
		
		
	}
	public void initialize(String userName,String password) throws MigrationException
	{
		appService = ApplicationServiceProvider.getApplicationService();
		authenticate(userName,password);
	}
	
	
	public void authenticate(String user,String password) throws MigrationException
	{
		try
		{
			clientSession = ClientSession.getInstance();
			clientSession.startSession(user, password);
		}
		catch(ApplicationException appExp)
		{
			throw new MigrationException(appExp.getMessage(),appExp);
		}
	}
	
	
	protected Object insertObject(Object obj) throws MigrationException
	{
		Object newObj=null;
		try
		{
			newObj = appService.createObject(obj);
		}
		catch(Exception appExp)
		{
			appExp.printStackTrace();
			throw new MigrationException(appExp.getMessage(),appExp);
		}
		return newObj;
	}
	
	public void deleteObject(Object obj) throws MigrationException
	{
		
	}
	
	public void updateObject(Object obj)throws MigrationException
	{
		
	}
	
	
	
	/**
	 * 
	 * @param idMap
	 */
	private static void insertMapEntries(ObjectIdentifierMap idMap)
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

	


}
