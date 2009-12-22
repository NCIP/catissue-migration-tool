package edu.wustl.migrator.util;

import java.util.ResourceBundle;


public class MigrationProperties
{

	private static ResourceBundle bundle;
	//private static Logger logger = edu.wustl.common.util.logger.Logger.getLogger(ApplicationProperties.class);
	public static void initBundle(String baseName)
	{
		bundle = ResourceBundle.getBundle(baseName);
	
	}

	public static String getValue(String theKey)
	{
		String val="";
		if(bundle == null)
		{
			//logger.fatal("resource bundle is null cannot return value for key " + theKey);
		}
		else
		{
			val= bundle.getString(theKey);
		}
		return val;
	}
}
