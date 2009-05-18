package edu.wustl.migrator.appservice;

import gov.nih.nci.system.applicationservice.ApplicationException;
import gov.nih.nci.system.applicationservice.ApplicationService;
import gov.nih.nci.system.applicationservice.ApplicationServiceProvider;
import gov.nih.nci.system.comm.client.ClientSession;


public class AppServiceImp
{
	private static ApplicationService appService = null;
	 
	public static void initializeAppService()
	{
		appService = ApplicationServiceProvider.getRemoteInstance("http://localhost:8080/catissuecore/http/remoteService");
		ClientSession clientSession = ClientSession.getInstance();
		try
		{
			clientSession.startSession("admin@admin.com", "login123");
		}
		catch (ApplicationException e)
		{
			e.printStackTrace();
		}
	}
	public static ApplicationService getAppService()
	{
		return appService;
	}
	
	public static void insert(Object object) throws Exception
	{
		appService.createObject(object);
	}
	
	
  
}
