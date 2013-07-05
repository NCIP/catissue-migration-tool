
package edu.wustl.bulkoperator.util;

import java.io.Reader;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.List;

import edu.wustl.common.exception.ApplicationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.dao.HibernateDAO;
import edu.wustl.dao.JDBCDAO;
import edu.wustl.dao.daofactory.DAOConfigFactory;
import edu.wustl.dao.exception.DAOException;
import edu.wustl.dao.query.generator.ColumnValueBean;

public class DaoUtil {

	private static final Logger logger = Logger.getCommonLogger(DaoUtil.class);

	public static ApplicationException getApplicationException(Exception exception,
			String errorName, String msgValues) {
		return new ApplicationException(ErrorKey.getErrorKey(errorName), exception, msgValues);

	}

	public static List executeSQLQuery(String sql, Object ... params) {
		JDBCDAO jdbcDao = null;
		
		try {
			jdbcDao = getJdbcDao();
			List<ColumnValueBean> paramVals = new ArrayList<ColumnValueBean>();
			
			if (params != null) {
				for (Object param : params) {
					paramVals.add(new ColumnValueBean(param));
				}
			}
			
			return jdbcDao.executeQuery(sql, paramVals);
		} catch (Exception e) {
			throw new RuntimeException("Error executing query: " + sql, e);
		} finally {
			closeJdbcDao(jdbcDao);
		}
	}
	
	public static String getString(Clob clob) {
		String result = null;
		if (clob != null) {
			Reader reader = null;
			try {
				StringBuilder clobValue = new StringBuilder();
				reader = clob.getCharacterStream();
				char[] buff = new char[1024];
				int read = 0;
				while ((read = reader.read(buff, 0, 1024)) > 0) {
					clobValue.append(buff, 0, read);
				}				
				
				result = clobValue.toString();
			} catch (Exception e) {
				throw new RuntimeException("Error reading string from clob column", e);
			} finally {
				if (reader != null) {
					try { reader.close(); } catch (Exception e) { }
				}
			}
		}
		
		return result;
	}
	
	public static JDBCDAO getJdbcDao() 
	throws Exception {
		String applicationName = CommonServiceLocator.getInstance().getAppName();
		JDBCDAO jdbcDao = DAOConfigFactory.getInstance().getDAOFactory(applicationName).getJDBCDAO();
		jdbcDao.openSession(null);
		return jdbcDao;
	}

	public static void closeJdbcDao(JDBCDAO jdbcDao) {
		try {
			if (jdbcDao != null) {
				jdbcDao.closeSession();
			}
		} catch (Exception e) {
			logger.error("Error closing JDBC dao session", e);
			throw new RuntimeException("Error closing JDBC dao session", e);
		}
	}	
}
