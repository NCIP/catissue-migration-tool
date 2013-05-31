/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.bulkoperator.util;

import java.util.List;

import edu.wustl.common.exception.ApplicationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.dao.JDBCDAO;
import edu.wustl.dao.daofactory.DAOConfigFactory;
import edu.wustl.dao.exception.DAOException;


public class AppUtility
{
	private static Logger logger = Logger.getCommonLogger(AppUtility.class);
	
	public static ApplicationException getApplicationException(Exception exception,
			String errorName, String msgValues)
	{
		return new ApplicationException(ErrorKey.getErrorKey(errorName), exception, msgValues);

	}
	
	public static JDBCDAO openJDBCSession() throws ApplicationException
	{
		JDBCDAO jdbcDAO = null;
		try
		{
			final String applicationName = CommonServiceLocator.getInstance().getAppName();
			jdbcDAO = DAOConfigFactory.getInstance().getDAOFactory(applicationName).getJDBCDAO();
			jdbcDAO.openSession(null);
		}
		catch (final DAOException daoExp)
		{
			AppUtility.logger.error(daoExp.getMessage(), daoExp);
			daoExp.printStackTrace();
			throw getApplicationException(daoExp, daoExp.getErrorKeyName(), daoExp
					.getErrorKeyName());
		}
		return jdbcDAO;
	}
	
	public static void closeJDBCSession(JDBCDAO jdbcDAO) throws ApplicationException
	{
		try
		{
			if (jdbcDAO != null)
			{
				jdbcDAO.closeSession();
			}
		}
		catch (final DAOException daoExp)
		{
			AppUtility.logger.error(daoExp.getMessage(), daoExp);
			daoExp.printStackTrace();
			throw getApplicationException(daoExp, daoExp.getErrorKeyName(), daoExp.getMsgValues());
		}
	}
	
	/**
	 * Executes sql Query and returns the results.
	 *
	 * @param sql
	 *            String hql
	 * @throws DAOException
	 *             DAOException
	 * @throws ClassNotFoundException
	 *             ClassNotFoundException
	 */
	public static List executeSQLQuery(String sql) throws ApplicationException
	{
		final JDBCDAO jdbcDAO = openJDBCSession();
		final List list = jdbcDAO.executeQuery(sql);
		closeJDBCSession(jdbcDAO);
		return list;
	}
}
