package edu.wustl.migrator.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.hibernate.Session;

import edu.wustl.common.lookup.DefaultLookupResult;
import edu.wustl.migrator.appservice.CaCoreMigrationAppServiceImpl;
import edu.wustl.migrator.dao.SandBoxDao;

public class MigrationUtility
{
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
		return System.currentTimeMillis()/1000;
	}
	public static boolean  participantMatching(CaCoreMigrationAppServiceImpl caCoreMigrationAppSerive,Object participant) throws Exception
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
	public static void storeMatchedParticipantRecords(Long id,List<Long> matchedParticipant)
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
			FileInputStream propFile = new FileInputStream(MigrationConstants.MIGRATION_INSTALL_PROPERTIES_FILE);
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
}