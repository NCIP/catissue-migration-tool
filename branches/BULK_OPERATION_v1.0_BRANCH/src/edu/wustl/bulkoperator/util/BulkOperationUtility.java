
package edu.wustl.bulkoperator.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.hibernate.Session;

import edu.wustl.bulkoperator.metadata.Attribute;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.dao.JDBCDAO;
import edu.wustl.dao.daofactory.DAOConfigFactory;
import edu.wustl.dao.exception.DAOException;

public class BulkOperationUtility
{
	/**
	 * logger Logger - Generic logger.
	 */
	private static final Logger logger = Logger.getCommonLogger(BulkOperationUtility.class);
	/**
	 * 
	 * @param bulkOperationclass
	 * @param columnNameHashTable
	 * @return
	 */
	public static String createHQL(BulkOperationClass bulkOperationclass,
			Hashtable<String, String> columnNameHashTable)
	{
		Iterator<Attribute> attributeItertor = bulkOperationclass.getAttributeCollection()
				.iterator();
		int count = 0;
		List<String> whereClause = new ArrayList<String>();

		while (attributeItertor.hasNext())
		{
			Attribute attribute = attributeItertor.next();
			if (attribute.getUpdateBasedOn())
			{
				String dataType = attribute.getDataType();
				String name = attribute.getName();
				String csvData = columnNameHashTable.get(attribute.getCsvColumnName());
				if (csvData != null && !"".equals(csvData))
				{
					if ("java.lang.String".equals(dataType) || "java.util.Date".equals(dataType))
					{
						whereClause.add(name + " = '" + csvData + "' ");
					}
					else
					{
						whereClause.add(name + " = " + csvData);
					}
				}
			}
		}
		StringBuffer hql = null;
		if (!whereClause.isEmpty())
		{
			hql = new StringBuffer(" from " + bulkOperationclass.getClassName() + " ");
			hql.append("where ");

			for (int i = 0; i < whereClause.size(); i++)
			{
				hql.append(whereClause.get(i));
				if (i != whereClause.size() - 1)
				{
					hql.append(" AND ");
				}
			}
		}
		System.out.println("---------- " + hql + " --------------");
		return hql.toString();
	}

	public static List<String> getAttributeList(BulkOperationClass bulkOperationClass, String suffix)
	{
		List<String> attributeList = new ArrayList<String>();
		Iterator<Attribute> attributeItertor = bulkOperationClass.getAttributeCollection()
				.iterator();
		while (attributeItertor.hasNext())
		{
			Attribute attribute = attributeItertor.next();
			attributeList.add(attribute.getCsvColumnName() + suffix);
		}

		Iterator<BulkOperationClass> containmentItert = bulkOperationClass
				.getContainmentAssociationCollection().iterator();
		while (containmentItert.hasNext())
		{
			BulkOperationClass containmentMigrationClass = containmentItert.next();
			List<String> subAttributeList = getAttributeList(containmentMigrationClass, suffix);
			attributeList.addAll(subAttributeList);
		}
		
		Iterator<BulkOperationClass> referenceItert = bulkOperationClass
				.getReferenceAssociationCollection().iterator();
		while (referenceItert.hasNext())
		{
			BulkOperationClass referenceMigrationClass = referenceItert.next();
			List<String> subAttributeList = getAttributeList(referenceMigrationClass, suffix);
			attributeList.addAll(subAttributeList);
		}
		return attributeList;
	}
	/**
	 * 
	 * @param name
	 * @return
	 */
	public static String getGetterFunctionName(String name)
	{
		String functionName = null;
		if (name != null && name.length() > 0)
		{
			String firstAlphabet = name.substring(0, 1);
			String upperCaseFirstAlphabet = firstAlphabet.toUpperCase();
			String remainingString = name.substring(1);
			functionName = "get" + upperCaseFirstAlphabet + remainingString;
		}
		return functionName;
	}

	public static String getSpecimenClassDomainObjectName(String name)
	{
		String objectName = null;
		if (name != null && name.length() > 0)
		{
			String firstAlphabet = name.substring(0, 1);
			String upperCaseFirstAlphabet = firstAlphabet.toUpperCase();
			String remainingString = name.substring(1);
			objectName = "edu.wustl.catissuecore.domain." + upperCaseFirstAlphabet
					+ remainingString + "Specimen";
		}
		return objectName;
	}

	public static String getSetterFunctionName(String name)
	{
		String functionName = null;
		if (name != null && name.length() > 0)
		{
			String firstAlphabet = name.substring(0, 1);
			String upperCaseFirstAlphabet = firstAlphabet.toUpperCase();
			String remainingString = name.substring(1);
			functionName = "set" + upperCaseFirstAlphabet + remainingString;
		}
		return functionName;
	}

	public static Long getTime()
	{
		return System.currentTimeMillis() / 1000;
	}

	public static void modifyData(String query, Session session) //throws SQLException
	{
		Connection connection = session.connection();
		Statement statement = null;
		try
		{
			statement = connection.createStatement();
			statement.executeUpdate(query);
			connection.commit();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			try
			{
				connection.rollback();
			}
			catch (SQLException e1)
			{
				e1.printStackTrace();
			}
		}
		finally
		{
			try
			{
				statement.close();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param query 
	 * String , query whose result is to be evaluate
	 * @return result set 
	 */
	public static Properties getMigrationInstallProperties()
	{
		Properties props = new Properties();
		try
		{
			FileInputStream propFile = new FileInputStream(
					BulkOperationConstants.MIGRATION_INSTALL_PROPERTIES_FILE);
			props.load(propFile);
		}
		catch (FileNotFoundException fnfException)
		{
			fnfException.printStackTrace();
		}
		catch (IOException ioException)
		{
			ioException.printStackTrace();
		}
		return props;
	}

	/**
	 * 
	 * @param csvFile
	 * @param zipFileName
	 * @return
	 * @throws IOException
	 */
	public File createZip(File csvFile, String zipFileName) throws BulkOperationException
	{
		File zipFile = null;
		try
		{		
			if (!csvFile.exists())
			{
				throw new FileNotFoundException("CSV File Not Found");
			}
			byte[] buffer = new byte[18024];
			 zipFile = new File(zipFileName + ".zip");
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFile));
			out.setLevel(Deflater.DEFAULT_COMPRESSION);
			FileInputStream in = new FileInputStream(csvFile);
			ZipEntry zipEntry = new ZipEntry(csvFile.getName());
			out.putNextEntry(zipEntry);
			int len;
			while ((len = in.read(buffer)) > 0)
			{
				out.write(buffer, 0, len);
			}
			out.closeEntry();
			in.close();
			out.close();
			csvFile.delete();
			if (csvFile.delete())
			{
				logger.info("CSV DELETED....");
			}
		}
		catch (FileNotFoundException fnfExp)
		{
			logger.error("Error while creating ouput report zip file.", fnfExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.zip.file");
			throw new BulkOperationException(errorkey, fnfExp, "");
		}
		catch (IOException ioExp)
		{
			logger.error("Error while creating ouput report zip file.", ioExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.zip.file");
			throw new BulkOperationException(errorkey, ioExp, "");
		}
		return zipFile;
	}
	/**
	 * 
	 * @return
	 */
	public String getUniqueKey()
	{
		Date date = new Date();
		Format formatter = new SimpleDateFormat("dd-MM-yy");
		return formatter.format(date);
	}
	public static Properties getBulkOperationProperties() throws BulkOperationException
	{
		Properties props = new Properties();
		try
		{
			FileInputStream propFile = new FileInputStream(
					BulkOperationConstants.CATISSUE_INSTALL_PROPERTIES_FILE);
			props.load(propFile);
		}
		catch (FileNotFoundException fnfException)
		{
			logger.debug("Error while accessing caTissueInstall.properties file.", fnfException);
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.file.not.found");
			throw new BulkOperationException(errorKey, fnfException, "caTissueInstall.properties");
		}
		catch (IOException ioException)
		{			
			logger.debug("Error while accessing caTissueInstall.properties file.", ioException);
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.file.reading.error");
			throw new BulkOperationException(errorKey, ioException, "caTissueInstall.properties");
		}
		return props;
	}
	/**
	 * 
	 * @return
	 * @throws BulkOperationException
	 */
	public static String getClassNameFromBulkOperationPropertiesFile() throws BulkOperationException
	{
		String fileName = System.getProperty("bulkoperator.appservice.class");
		Properties properties = BulkOperationUtility.getPropertiesFile(fileName);
		return properties.getProperty(BulkOperationConstants.BULK_OPERATION_APPSERVICE_CLASSNAME);		
	}
	/**
	 * Get CatissueInstallProperties.
	 * @return Properties.
	 */
	public static Properties getPropertiesFile(String propertiesFileName) throws BulkOperationException
	{
		Properties props = new Properties();
		try
		{
			FileInputStream propFile = new FileInputStream(propertiesFileName);
			props.load(propFile);
		}
		catch (FileNotFoundException fnfException)
		{
			logger.debug("caTissueInstall.properties file not found.", fnfException);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.file.not.found");
			throw new BulkOperationException(errorkey, null, "caTissueInstall.properties");
		}
		catch (IOException ioException)
		{			
			logger.debug("Error while accessing caTissueInstall.properties file.", ioException);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.file.reading.error");
			throw new BulkOperationException(errorkey, null, "caTissueInstall.properties");
		}
		return props;
	}
	/**
	 * Get Database Type.
	 * @return String.
	 */
	public static String getDatabaseType() throws BulkOperationException
	{
		Properties properties = getPropertiesFile(
				BulkOperationConstants.CATISSUE_INSTALL_PROPERTIES_FILE);
		return properties.getProperty("database.type");
	}
	/**
	 * This method will change the Bulk Operation status from In Progress
	 * to Failed. The method should be called whenever the application 
	 * server and stops.
	 * @param sessionData SessionDataBean
	 */
	public static void changeBulkOperationStatusToFailed() throws DAOException
	{
		try
		{
			final String appName = CommonServiceLocator.getInstance().getAppName();
			final JDBCDAO jdbcDao = DAOConfigFactory.getInstance().getDAOFactory(appName)
					.getJDBCDAO();
			jdbcDao.openSession(null);
			jdbcDao.executeUpdate("update job_details set job_status = 'Failed' where job_status = 'In Progress'");
			jdbcDao.commit();
			jdbcDao.closeSession();
		}
		catch (final DAOException daoExp)
		{
			logger.error("Could not update the table Job Details with the " +
				"status column value from inprogess to failed." + daoExp.getMessage(), daoExp);
			daoExp.printStackTrace();
			throw daoExp;
		}
	}
}