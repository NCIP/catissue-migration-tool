package edu.wustl.migrator.util;


public class MigrationException extends Exception
{
	public MigrationException()
	{
		super();
	}
	
	public MigrationException(String message)
	{
		super(message);
	}

	public MigrationException(String message,Throwable throwable)
	{
		super(message, throwable);
	}
	
	public MigrationException(Throwable throwable)
	{
		super(throwable);
		
	}
	
}
