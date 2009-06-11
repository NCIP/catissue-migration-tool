
package edu.wustl.migrator.appservice;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import edu.wustl.catissuecore.domain.Container;
import edu.wustl.catissuecore.domain.Institution;
import edu.wustl.migrator.MigrationObjectStatusHandler;
import edu.wustl.migrator.dao.DAO;
import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationException;
import gov.nih.nci.system.applicationservice.ApplicationException;
import gov.nih.nci.system.applicationservice.ApplicationService;
import gov.nih.nci.system.applicationservice.ApplicationServiceProvider;
import gov.nih.nci.system.comm.client.ClientSession;

public class CaCoreMigrationAppServiceImpl extends MigrationAppService
{

	ApplicationService appService;
	
	public ApplicationService getAppService()
	{
		return appService;
	}
	
	public void setAppService(ApplicationService appService)
	{
		this.appService = appService;
	}

	ClientSession clientSession;
	
	public CaCoreMigrationAppServiceImpl(boolean isAuthenticationRequired,String userName,String password) throws MigrationException
	{
		super(isAuthenticationRequired, userName, password);
	}
	public void initialize(String userName,String password) throws MigrationException
	{
		//appService = ApplicationServiceProvider.getApplicationService();
		appService = ApplicationServiceProvider.getRemoteInstance("http://localhost:8080/catissuecore/http/remoteService");
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
			/*System.out.println("appService:"+appService);
			Institution INS = new Institution();
			INS.setId(new Long(1));
			List list = appService.search("edu.wustl.catissuecore.domain.Institution", INS);*/
			newObj = appService.createObject(obj);
		}
		catch(Exception appExp)
		{
			//appExp.printStackTrace();
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
	
}
