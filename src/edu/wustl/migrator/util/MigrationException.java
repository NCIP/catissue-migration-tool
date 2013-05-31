/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

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
