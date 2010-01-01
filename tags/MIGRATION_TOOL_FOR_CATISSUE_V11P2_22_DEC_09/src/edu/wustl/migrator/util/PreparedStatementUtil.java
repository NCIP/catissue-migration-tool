package edu.wustl.migrator.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.wustl.migrator.dao.SandBoxDao;


public class PreparedStatementUtil
{

	private static Map<String, PreparedStatement> preparedStmentsMap = new HashMap<String, PreparedStatement>();

	public static PreparedStatement getPreparedStatement(String sql) throws SQLException
	{
		if (preparedStmentsMap.containsKey(sql))
		{
			return preparedStmentsMap.get(sql);
		}
		else
		{
			PreparedStatement ps = SandBoxDao.getConnection().prepareStatement(sql);
			preparedStmentsMap.put(sql, ps);
			return ps;
		}
	}
	public static void closePreparedStatements()
	{
		Iterator<String> iterator = preparedStmentsMap.keySet().iterator();
		while(iterator.hasNext())
		{
			try
			{
				String key = iterator.next();
				preparedStmentsMap.get(key).close();
				//preparedStmentsMap.remove(key);
				//cannot remove remove map as concrete access and remove not allowed
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
			finally
			{
				preparedStmentsMap = new HashMap<String, PreparedStatement>();
			}
		}
	}
}