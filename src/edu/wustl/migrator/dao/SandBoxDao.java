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
	private static SessionFactory sessionFactory = null;
	
	public static void init() throws MigrationException
	{
		try
		{
		sessionFactory = new Configuration().configure(
				"hibernateForStagingDb.cfg.xml").buildSessionFactory();
		session = sessionFactory.openSession();
		session.setFlushMode(FlushMode.NEVER);
		
		}
		catch(Exception e)
		{			
			e.printStackTrace();
			throw new MigrationException(e.getMessage(),e);
		}
	}

/*	public static Session getCurrentSession()
	{
		return session;
	}*/
//	public static void initializeIdMap()
//	{
//		String className =null;
//		Transaction trasaction = session.beginTransaction();
//		for(int i = 0 ; i < 5 ; i++)
//		{
//			if(i==0)
//			{
//				className =  "edu.wustl.catissuecore.domain.Department";
//			}
//			if(i==1)
//			{
//				className =  "edu.wustl.catissuecore.domain.Institution";
//			}
//			if(i==2)
//			{
//				className =  "edu.wustl.catissuecore.domain.CancerResearchGroup";
//			}
//			if(i==3)
//			{
//				className =  "edu.wustl.catissuecore.domain.Site";
//			}
//			if(i==4)
//			{
//				className =  "edu.wustl.catissuecore.domain.User";
//			}
//		
//		ObjectIdentifierMap idMap = new ObjectIdentifierMap(className);
//		idMap.setOldId(new Long(1));
//		idMap.setNewId(new Long(1));
//		
//		session.save(idMap);
//		}
//		trasaction.commit();
//		
//	}
	public static void closeSession()
	{
		session.close();
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
		Transaction transaction = session.getTransaction();
		transaction.begin();
		session.save(obj);
		transaction.commit();
	}
	
	public static void executeSQLUpdate(String sql)
	{
		
	}
	
	public static List executeHQLQuery(String hql)
	{
		List returnObjectList = session.createQuery(hql).list();
		return returnObjectList;
	}
	
	
	public static Long getProductionId(Long sandBoxId, String className)
	{
		Long productionId = null;
		String sqlQuery = "select new_id from catissue_migration_mapping where object_className='"
				+ className + "' and old_id=" + sandBoxId;
		List result = SandBoxDao.executeSQLQuery(sqlQuery);
		if (result != null && !result.isEmpty())
		{
			productionId = Long.valueOf(result.get(0).toString());
		}
		return productionId;
	}
	public static void insertMapEntries(String className, Long oldId, Long newId)
	{
		ObjectIdentifierMap idMap = new ObjectIdentifierMap(className);
		idMap.setOldId(oldId);
		idMap.setNewId(newId);
		SandBoxDao.saveObject(idMap);
		/*String sqlQuery = "insert into catissue_migration_mapping (object_classname, old_Id, new_Id) values ('"
				+ className + "'," + oldId + "," + newId + ")";
		SandBoxDao.executeSQLQuery(sqlQuery);*/
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
	public static Session getNewSession()
	{
		Session newSession = null;
		try
		{
		sessionFactory = new Configuration().configure(
				"hibernateForStagingDb.cfg.xml").buildSessionFactory();
		newSession = sessionFactory.openSession();
		newSession.setFlushMode(FlushMode.NEVER);
		
		}
		catch(Exception e)
		{			
			e.printStackTrace();
			//throw new MigrationException(e.getMessage(),e);
		}
		return newSession;
	}
}
