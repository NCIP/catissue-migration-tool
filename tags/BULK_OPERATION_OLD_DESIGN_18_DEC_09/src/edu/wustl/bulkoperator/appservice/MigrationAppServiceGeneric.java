package edu.wustl.bulkoperator.appservice;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

import edu.wustl.bulkoperator.util.BulkOperationException;


public abstract class MigrationAppServiceGeneric extends MigrationAppService
{
	public MigrationAppServiceGeneric(boolean isAuthenticationRequired, String userName,
			String password) throws BulkOperationException
	{
		super(isAuthenticationRequired, userName, password);
		// TODO Auto-generated constructor stub
	}
	private Session session;

	public void deleteObject(Object obj)
	{
		// TODO Auto-generated method stub

	}

	public Object insertObject(Object obj)
	{
		try
		{
			if (obj != null && obj instanceof Collection)
			{
				session = getNewSession();
				Transaction transaction = session.beginTransaction();
				Iterator iterator = ((Collection) obj).iterator();
				while (iterator.hasNext())
				{
					Object singleObj = iterator.next();
					transaction.begin();
					checkForTransientRef(singleObj);
					session.save(singleObj);
					transaction.commit();
				}
			}
			else if (obj != null)
			{
				session = getNewSession();
				Transaction transaction = session.beginTransaction();
				transaction.begin();
				checkForTransientRef(obj);
				session.save(obj);
				transaction.commit();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			session.close();
		}
		return null;
	}

	public Object updateObject(Object obj)
	{
		try {
			
		session = getNewSession();
		Transaction transaction = session.beginTransaction();
		transaction.begin();
		
			checkForTransientRef(obj);
			session.save(obj);
			transaction.commit();
			
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	public  Session getNewSession()
	{
		try
		{
		SessionFactory sessionFactory = new Configuration().configure(
				"hibernate.cfg.xml").buildSessionFactory();
		session = sessionFactory.openSession();
		}
		catch(Exception e)
		{e.printStackTrace();}
		return session;
	}
	public void checkForTransientRef(Object obj) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
	{
		Class objClass = obj.getClass();
		Method allMethods[] = objClass.getMethods();
		int noOfMethods = allMethods.length;
		for (int i = 0; i < noOfMethods; i++)
		{
			Method method = allMethods[i];
			String methodName = method.getName();
			String getOrSet = methodName.substring(0, 2);
			if (getOrSet.equalsIgnoreCase("get"))
			{
				Object object = method.invoke(obj, null);
				if (object != null && object instanceof Collection)
				{
					Iterator iterator = ((Collection) obj).iterator();
					while (iterator.hasNext())
					{
						Object singleObj = iterator.next();
						try
						{
							Method getId = object.getClass().getMethod("getId", null);
							Object id = getId.invoke(object, null);
							if(id == null)
							{
								session.save(object);
							}
						}
						catch (SecurityException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						catch (NoSuchMethodException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					

				}
				else if (object != null)
				{
					try
					{
						Method getId = object.getClass().getMethod("getId", null);
						Object id = getId.invoke(object, null);
						if(id == null)
						{
							session.save(object);
						}
					}
					catch (SecurityException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					catch (NoSuchMethodException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}
		}
	}

}
