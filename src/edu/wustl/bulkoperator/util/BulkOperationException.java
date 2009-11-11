package edu.wustl.bulkoperator.util;


public class BulkOperationException extends Exception
{
	public BulkOperationException()
	{
		super();
	}
	
	public BulkOperationException(String message)
	{
		super(message);
	}

	public BulkOperationException(String message,Throwable throwable)
	{
		super(message, throwable);
	}
	
	public BulkOperationException(Throwable throwable)
	{
		super(throwable);
		
	}
	
}
