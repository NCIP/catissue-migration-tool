package edu.wustl.migrator.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;



//This class helps in establishing and destroying DB connections
public class DAO
{  
	//function to establish DB connection
	public Connection establishConnection() 
	{
		Connection conn=null;
		String driver = "com.mysql.jdbc.Driver" , jdbcURL = "jdbc:mysql://localhost:3306/pcatissuecore_db_by_sachin" , username = "root" , password = "root";
		
		try 
		{
			Class.forName(driver);
			conn = DriverManager.getConnection(jdbcURL,username,password);
		}
		catch (ClassNotFoundException e) 
		{
			e.printStackTrace();
		}
		catch (SQLException e) 
		{
			e.printStackTrace();
		}		    
	   
	   return conn;
	}
	
	//function to destroy a given connection
	public void destroyConnection(Connection conn)
	{
		try 
		{
			conn.close();
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
	}
}
