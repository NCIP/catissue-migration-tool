package edu.wustl.bulkoperator.util;

public class MigratorThread extends Thread
{
	public static boolean pleaseWait = true;
	/**
	 * 
	 */
	public MigratorThread()
	{
		super();
	}

	/**
	 * 
	 */
	public void run()
	{
		try
		{
			while(pleaseWait)
			{
				System.gc();
				System.out.println("Garbage Collector Called");
				sleep(600000);
			}
		}
		catch (InterruptedException e)
		{
			MigratorThread.pleaseWait = false;
			System.out.println("Garbage Collector Called Off");
		}		
	}
}