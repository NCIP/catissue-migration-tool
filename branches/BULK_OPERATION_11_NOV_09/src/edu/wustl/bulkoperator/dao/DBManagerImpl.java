package edu.wustl.bulkoperator.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.catissuecore.util.global.Constants;
import edu.wustl.common.util.logger.Logger;

/**
 * Database manager for Bulk Operation from UI.
 * @author sagar_baldwa
 *
 */
public class DBManagerImpl
{
	/**
	 * logger Logger - Generic logger.
	 */
	private static final Logger logger = Logger.getCommonLogger(DBManagerImpl.class);
	/**
	 * Get Connection Object.
	 * @return Connection.
	 * @throws BulkOperationException BulkOperationException.
	 */
	public static Connection getConnection() throws BulkOperationException
	{
		Properties properties = BulkOperationUtility.getCatissueInstallProperties();
		return getConnection(properties);
	}

	/**
	 * Get Connection Object.
	 * @param properties Properties.
	 * @return Connection.
	 * @throws BulkOperationException BulkOperationException.
	 */
	private static Connection getConnection(Properties properties)
		throws BulkOperationException 
	{
		Connection connection = null;
		try
		{
			String databaseDriver = null;
			String databaseType = properties.getProperty("database.type");
			String databaseHost = properties.getProperty("database.host");
			String databasePort = properties.getProperty("database.port");
			String databaseName = properties.getProperty("database.name");
			String databaseUsername = properties.getProperty("database.username");
			String databasePassword = properties.getProperty("database.password");
			String databaseURL = null;
			if(Constants.ORACLE_DATABASE.equalsIgnoreCase(databaseType))
			{
				databaseDriver = "oracle.jdbc.driver.OracleDriver";
				databaseURL = "jdbc:oracle:thin:@"+databaseHost+":"+databasePort+":"+databaseName;
			}
			else if(Constants.MYSQL_DATABASE.equalsIgnoreCase(databaseType))
			{
				databaseDriver = "com.mysql.jdbc.Driver";
				databaseURL = "jdbc:mysql://"+databaseHost+":"+databasePort+"/"+databaseName;
			}
			Class.forName(databaseDriver);
			connection = DriverManager.getConnection(databaseURL,
										databaseUsername, databasePassword);
		}
		catch (ClassNotFoundException cnfExp)
		{
			logger.debug("Error in creating database connection." +
					" Please check the database driver.", cnfExp);
			throw new BulkOperationException("Error in creating database connection." +
					" Please check the database driver.", cnfExp);
		}
		catch (SQLException sqlExp)
		{
			logger.debug("Error in creating database connection.", sqlExp);
			throw new BulkOperationException("Error in creating database connection.", sqlExp);
		}
		catch (Exception exp)
		{
			logger.debug("Error in creating database connection." +
				" Please check the database driver or the caTissueInstall.properties have some " +
				"missing database properties.", 
					exp);
			throw new BulkOperationException("Error in creating database connection." +
					" Please check the database driver or the caTissueInstall.properties have some " +
					"missing database properties.", exp);
		}
		return connection;
	}
}