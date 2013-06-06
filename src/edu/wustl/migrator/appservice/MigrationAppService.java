/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue-migration-tool/LICENSE.txt for details.
 */

package edu.wustl.migrator.appservice;

import java.lang.reflect.InvocationTargetException;

import edu.wustl.migrator.MigrationObjectStatusHandler;
import edu.wustl.migrator.dao.SandBoxDao;
import edu.wustl.migrator.metadata.MigrationClass;
import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationException;


public abstract class  MigrationAppService
{
	public MigrationAppService(boolean isAuthenticationRequired,String userName,String password) throws MigrationException
	{
		this.isAuthenticationRequired=isAuthenticationRequired;
		initialize(userName, password);
	}
	
	protected boolean isAuthenticationRequired = true;
	
	public boolean isAuthenticationRequired()
	{
		return isAuthenticationRequired;
	}
	
	abstract public void initialize(String userName,String password) throws MigrationException;

	abstract public void authenticate(String userName,String password) throws MigrationException;
	
	public void insert(Object obj,MigrationClass migration,ObjectIdentifierMap objectIdentifierMap) throws MigrationException
	{		
		try
		{
			SandBoxDao.getCurrentSession().clear();
			Object newObj = insertObject(obj);
			objectIdentifierMap.setNewId((Long)migration.invokeGetIdMethod(newObj));
			MigrationObjectStatusHandler.getInstance().handleSuccessfullyMigratedObject(newObj, migration, objectIdentifierMap);
		}	
		catch(Exception appExp)
		{
			try
			{
				MigrationObjectStatusHandler.getInstance().handleFailedMigrationObject(obj,migration,appExp.getMessage(),appExp);
			}
			catch (IllegalArgumentException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (SecurityException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (InvocationTargetException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (NoSuchMethodException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//appExp.printStackTrace();
		}
		
	}
	
	abstract protected Object insertObject(Object obj) throws MigrationException ;
	
	abstract public void deleteObject(Object obj) throws MigrationException;
	
	abstract public void updateObject(Object obj) throws MigrationException;

}
