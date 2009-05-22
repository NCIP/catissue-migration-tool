package edu.wustl.migrator.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

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
	}

	public static void closeSession()
	{
		session.close();
	}*/
	
	public static List<?>  executeSQLQuery(String sql)
	{
		List<?> returnObjectList = null;
		returnObjectList = session.createSQLQuery(sql).list();
		return returnObjectList;
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
			productionId = (Long) result.get(0);
		}
		return productionId;
	}
}
