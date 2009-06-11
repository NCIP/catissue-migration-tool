package edu.wustl.migrator.dao;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.hibernate.FlushMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

import edu.wustl.migrator.metadata.ObjectIdentifierMap;
import edu.wustl.migrator.util.MigrationException;


public class SandBoxDao
{
	private static Session session = null;
	private static Session insertionSession = null;
	
	public static Session getInsertionSession()
	{
		return insertionSession;
	}
	
	public static void setInsertionSession(Session insertionSession)
	{
		SandBoxDao.insertionSession = insertionSession;
	}
	private static SessionFactory sessionFactory = null;
	
	public static void init() throws MigrationException
	{
		try
		{
		sessionFactory = new Configuration().configure(
				"hibernateForStagingDb.cfg.xml").buildSessionFactory();
		session = sessionFactory.openSession();
		insertionSession = sessionFactory.openSession();
		session.setFlushMode(FlushMode.NEVER);
		}
		catch(Exception e)
		{			
			e.printStackTrace();
			throw new MigrationException(e.getMessage(),e);
		}
	}
	public static void getNewSession()
	{
		session = sessionFactory.openSession();
		session.setFlushMode(FlushMode.NEVER);
	}
	public static Session getCurrentSession()
	{
		return session;
	}
	public static void initializeIdMap()
	{
		String className =null;
		Transaction trasaction = insertionSession.beginTransaction();
		for(int i = 0 ; i < 5 ; i++)
		{
			if(i==0)
			{
				className =  "edu.wustl.catissuecore.domain.Department";
			}
			if(i==1)
			{
				className =  "edu.wustl.catissuecore.domain.Institution";
			}
			if(i==2)
			{
				className =  "edu.wustl.catissuecore.domain.CancerResearchGroup";
			}
			if(i==3)
			{
				className =  "edu.wustl.catissuecore.domain.Site";
			}
			if(i==4)
			{
				className =  "edu.wustl.catissuecore.domain.User";
			}
		
		ObjectIdentifierMap idMap = new ObjectIdentifierMap(className);
		idMap.setOldId(new Long(1));
		idMap.setNewId(new Long(1));
		
		insertionSession.save(idMap);
		}
		trasaction.commit();
		
	}
	public static void closeSession()
	{
		session.close();
		session = null;
	}
	public static void closeInsertionSession()
	{
		insertionSession.close();
	}
	
	public static List<?>  executeSQLQuery(String sql)
	{
		List<?> returnObjectList = null;
		System.out.println("sql"+sql);
		returnObjectList = session.createSQLQuery(sql).list();
		return returnObjectList;
	}
	public static void saveObject(Object obj)
	{
		Transaction transaction = insertionSession.getTransaction();
		transaction.begin();
		insertionSession.save(obj);
		//insertionSession.flush();
		transaction.commit();
		
	}
	
	public static void executeSQLUpdate(String sql)
	{
		
	}
	
	public static List executeHQLQuery(String hql)
	{
		List returnObjectList = null;
		try
		{
			returnObjectList = session.createQuery(hql).list();

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return returnObjectList;
	}
	
	
	public static Long getProductionId(Long sandBoxId, String className)
	{
		Long productionId = null;
		String sqlQuery = "select new_id from migration_mapping where object_className='"
				+ className + "' and old_id=" + sandBoxId;
		List result = executeSQLQuery(sqlQuery);
		if (result != null && !result.isEmpty())
		{
			productionId = Long.valueOf(result.get(0).toString());
		}
		return productionId;
	}
	public static void insertMapEntries(ObjectIdentifierMap idMap)
	{
		if(idMap.getOldId() != null && idMap.getNewId() != null)
		{
			saveObject(idMap);
			System.out.println("Map inserted for: "+idMap.getClassName()+" old id: "+idMap.getOldId() +" new id: "+idMap.getNewId());
		}
		else
		{
			System.out.println("Map not inserted for: "+idMap.getClassName()+" old id: "+idMap.getOldId() +" new id: "+idMap.getNewId());
		}
		
	}
	public static Object retrieve(Object obj) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
	{
		Object sandBoxObj = null;
		if(obj != null)
		{
			String className = obj.getClass().getName();
			try
			{
				Method getId = obj.getClass().getMethod("getId", null);
				Object id = getId.invoke(obj, null);
				if(id != null)
				{
					Long identifier = Long.valueOf(id.toString());
					String query = "from "+className+" where id="+identifier;
					Query q = session.createQuery(query);
					List result =q.list();
					if(result != null && !result.isEmpty())
					{
						sandBoxObj = result.get(0);
					}
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
		return sandBoxObj;
	}
	public static List retrieve(String className, String[] whereColumn, String[] whereValue,String[] returnColumn)
	{
		List result = null;
		if (className != null)
		{
			StringBuffer query = new StringBuffer();
			if(returnColumn != null)
			{
				query.append("select ");
				for(int i = 0 ; i < returnColumn.length ; i++)
				{
					if(i < returnColumn.length - 1)
					{
					query.append(returnColumn[i]+",");
					}
					else
					{
					query.append(returnColumn[i]);	
					}
				}
			}
			query.append(" from "+ className);
			if(whereColumn != null && whereValue != null && whereColumn.length == whereValue.length)
			{
				query.append(" where ");
				for(int i = 0 ; i < whereColumn.length ; i++)
				{
					if(i < whereColumn.length - 1)
					{
					query.append(whereColumn[i]+"="+whereValue[i]+" and ");
					}
					else
					{
						query.append(whereColumn[i]+"="+whereValue[i]);
					}
				}
			}
			System.out.println(query.toString());
			result = executeHQLQuery(query.toString());
			
		}
		return result;
	}
	public static void delete(Object obj)
	{
		Transaction transaction = insertionSession.getTransaction();
		transaction.begin();
		insertionSession.delete(obj);
		//insertionSession.flush();
		transaction.commit();
		
	}
	
}
