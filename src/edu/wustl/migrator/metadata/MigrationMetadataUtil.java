
package edu.wustl.migrator.metadata;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.exolab.castor.mapping.Mapping;
import org.exolab.castor.xml.Unmarshaller;

import edu.wustl.migrator.dao.DAO;

public class MigrationMetadataUtil
{
	//static ApplicationService appService = null;

	public  MigrationMetadata unmarshall()
	{
		MigrationMetadata migrate = null;
		try
		{
			// -- Load a mapping file
			Mapping mapping = new Mapping();
			mapping.loadMapping("mapping.xml");

			Unmarshaller un = new Unmarshaller(MigrationMetadata.class);
			un.setMapping(mapping);

			// -- Read in the migration.xml using the mapping
			FileReader in = new FileReader("migrationmetadata.xml");
			migrate = (MigrationMetadata) un.unmarshal(in);
			in.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e);
		}
		return migrate;
	}
	/**
	 * the function takes the migration class object and returns the set of ids 
	 * using the sql provided
	 * @param migrationObj
	 * @return
	 */
	public List<Long> migratingDataIDs(MigrationClass migrationObj)
	{
		Connection conn = null;
		DAO dao = new DAO();
		conn = dao.establishConnection();
		String sqlQuery = migrationObj.getSql();
		List<Long> objectList = new ArrayList<Long>();

		ResultSet rs = null;

		try
		{
			Statement stmt = conn.createStatement();
			rs = stmt.executeQuery(sqlQuery);
			int i = 1;
			for (; rs.next(); i++)
			{
				objectList.add(rs.getLong(1));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			dao.destroyConnection(conn);
		}

		return objectList;
	}
	
	
}
