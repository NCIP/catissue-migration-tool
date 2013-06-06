/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue-migration-tool/LICENSE.txt for details.
 */

package edu.wustl.migrator.util;

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