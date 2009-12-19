
package edu.wustl.bulkoperator.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
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

import au.com.bytecode.opencsv.CSVReader;
import edu.wustl.bulkoperator.appservice.CaCoreMigrationAppServiceImpl;
import edu.wustl.bulkoperator.dao.SandBoxDao;
import edu.wustl.bulkoperator.metadata.Attribute;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.common.lookup.DefaultLookupResult;
import edu.wustl.common.util.logger.Logger;

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

	public static List getAttributeList(BulkOperationClass bulkOperationClass, String suffix)
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

	public static boolean participantMatching(
			CaCoreMigrationAppServiceImpl caCoreMigrationAppSerive, Object participant)
			throws Exception
	{
		List list = caCoreMigrationAppSerive.getAppService().getParticipantMatchingObects(
				participant);
		List<Long> matchedParticipant = new ArrayList<Long>();
		if (list != null && list.size() > 0)
		{
			Long particiapntId = (Long) participant.getClass().getMethod("getId", null).invoke(
					participant, null);
			Iterator<DefaultLookupResult> it = list.iterator();
			while (it.hasNext())
			{
				DefaultLookupResult defaultLookupResult = it.next();
				Object match = defaultLookupResult.getObject();
				matchedParticipant.add((Long) match.getClass().getMethod("getId", null).invoke(
						match, null));
			}
			storeMatchedParticipantRecords(particiapntId, matchedParticipant);
			return true;
		}
		return false;
	}

	public static void storeMatchedParticipantRecords(Long id, List<Long> matchedParticipant)
	{
		for (int i = 0; i < matchedParticipant.size(); i++)
		{
			String query = "insert into  CONFLICTING_PARTICIPANT(SANDBOX_PARTICIPANT_ID,PRODUCTION_PARTICIPANT_ID) values("
					+ id + "," + matchedParticipant.get(i) + ")";
			modifyData(query, SandBoxDao.getInsertionSession());
		}
		//appendErrorLog(Participant.class.getName(), "domain.object.processor.ParticipantProcessor", id,"matching found with "+ matchedParticipant);
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
					MigrationConstants.MIGRATION_INSTALL_PROPERTIES_FILE);
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
	public File createZip(File csvFile, String zipFileName) throws IOException
	{	
		if (!csvFile.exists())
		{
			throw new FileNotFoundException("CSV File Not Found");
		}
		byte[] buffer = new byte[18024];
		File zipFile = new File(zipFileName + ".zip");
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
			//Logger.out.info("CSV DELETED....");
		}
		return zipFile;
		//Logger.out.info("ZIP FILE GENERATED....");
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
	/**
	 * Get CatissueInstallProperties.
	 * @return Properties.
	 */
	public static Properties getCatissueInstallProperties() throws BulkOperationException
	{
		Properties props = new Properties();
		try
		{
			FileInputStream propFile = new FileInputStream(
					MigrationConstants.CATISSUE_INTSALL_PROPERTIES_FILE);
			props.load(propFile);
		}
		catch (FileNotFoundException fnfException)
		{
			logger.debug("caTissueInstall.properties file not found.", fnfException);
			throw new BulkOperationException("caTissueInstall.properties file not found.");
		}
		catch (IOException ioException)
		{
			logger.debug("Error while accessing caTissueInstall.properties file.", ioException);
			throw new BulkOperationException("Error while accessing caTissueInstall.properties file.");
		}
		return props;
	}
	/**
	 * Get Database Type.
	 * @return String.
	 */
	public static String getDatabaseType() throws BulkOperationException
	{
		Properties properties = getCatissueInstallProperties();
		return properties.getProperty("database.type");
	}
}