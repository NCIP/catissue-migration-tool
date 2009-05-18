
package edu.wustl.migrator.dao;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class HibernateSessionHandler
{

	private static Session session = null;

	public static Session getCurrentSession()
	{
		return session;
	}

	public static void closeSession()
	{
		session.close();
	}

	public static Session getNewSession()
	{
		try
		{
		SessionFactory sessionFactory = new Configuration().configure(
				"hibernateForStagingDb.cfg.xml").buildSessionFactory();
		session = sessionFactory.openSession();
		}
		catch(Exception e)
		{e.printStackTrace();}
		return session;
	}

}
