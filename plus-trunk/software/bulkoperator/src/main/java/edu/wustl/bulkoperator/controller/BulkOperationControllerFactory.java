
package edu.wustl.bulkoperator.controller;


import edu.wustl.common.util.logger.Logger;

public class BulkOperationControllerFactory
{
	private static final Logger logger = Logger.getCommonLogger(BulkOperationControllerFactory.class);
	private static BulkOperationControllerFactory factory = null;

	private BulkOperationControllerFactory()
	{}

	public static BulkOperationControllerFactory getInstance()
	{
		if (factory == null)
		{
			factory = new BulkOperationControllerFactory();
		}
		return factory;
	}
}