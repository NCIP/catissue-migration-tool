package edu.wustl.migrator.appservice;

import java.util.List;


public interface MigrationAppService
{
	public void insertObject(Object obj);
	
	public void deleteObject(Object obj);
	
	public void updateObject(Object obj);

}
