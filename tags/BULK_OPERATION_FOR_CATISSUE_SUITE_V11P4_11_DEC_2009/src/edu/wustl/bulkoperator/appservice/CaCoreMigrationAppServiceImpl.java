
package edu.wustl.bulkoperator.appservice;

import java.util.List;

import edu.wustl.bulkoperator.util.BulkOperationException;
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
	
	public CaCoreMigrationAppServiceImpl(boolean isAuthenticationRequired,String userName,String password) throws BulkOperationException
	{
		super(isAuthenticationRequired, userName, password);
	}
	public void initialize(String userName,String password) throws BulkOperationException
	{
		appService = ApplicationServiceProvider.getApplicationService();
		authenticate(userName,password);
	}	
	
	public void authenticate(String user,String password) throws BulkOperationException
	{
		try
		{
			clientSession = ClientSession.getInstance();
			clientSession.startSession(user, password);
		}
		catch(ApplicationException appExp)
		{
			throw new BulkOperationException(appExp.getMessage(),appExp);
		}
	}
	
	
	protected Object insertObject(Object obj) throws BulkOperationException
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
			throw new BulkOperationException(appExp.getMessage(), appExp);	
		}
		return newObj;
	}
	
	public Object searchObject(Object obj)throws BulkOperationException
	{
		Object newObj = null;
		try
		{
			List list = appService.search(obj.getClass(), obj);
			if(list.isEmpty())
			{
				throw new BulkOperationException("Could not find the object with the specified Update based on values in XML");
			}
			newObj = list.get(0);
		}
		catch (ApplicationException e)
		{
			e.printStackTrace();
		}		
		return newObj;
	}
	
	public void deleteObject(Object obj) throws BulkOperationException
	{
		
	}
	
	public Object updateObject(Object obj)throws BulkOperationException
	{
		Object newObj=null;
		try
		{
			/*System.out.println("appService:"+appService);
			Institution INS = new Institution();
			INS.setId(new Long(1));
			List list = appService.search("edu.wustl.catissuecore.domain.Institution", INS);*/
			newObj = appService.updateObject(obj);
		}
		catch(Exception appExp)
		{
			throw new BulkOperationException(appExp.getMessage(), appExp);
		}
		return newObj;
	}
	
}
