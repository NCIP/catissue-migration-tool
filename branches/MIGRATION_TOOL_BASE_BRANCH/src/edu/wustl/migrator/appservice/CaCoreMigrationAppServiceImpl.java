
package edu.wustl.migrator.appservice;

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