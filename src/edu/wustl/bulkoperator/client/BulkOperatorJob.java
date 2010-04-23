
package edu.wustl.bulkoperator.client;

import java.io.InputStream;

import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.controller.BulkOperationProcessController;
import edu.wustl.bulkoperator.jobmanager.AbstractJob;
import edu.wustl.bulkoperator.jobmanager.JobStatusListener;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.common.util.logger.Logger;

public class BulkOperatorJob extends AbstractJob
{
	/**
	 * logger instance of the class.
	 */
	private final static Logger logger = Logger.getCommonLogger(BulkOperatorJob.class);
	private InputStream csvFileInputStream = null;
	private BulkOperationClass bulkOperationClass = null;
	private AppServiceInformationObject serviceInformationObject = null;

	public BulkOperatorJob(String operationName, String userId,
			InputStream csvFileInputStream, JobStatusListener jobStatusListener,
			AppServiceInformationObject serviceInformationObject, BulkOperationClass bulkOperationClass)
	{
		super(operationName, userId, jobStatusListener);
		this.serviceInformationObject =serviceInformationObject;
		this.csvFileInputStream = csvFileInputStream;
		this.bulkOperationClass = bulkOperationClass;
	}

	@Override
	public void doJob()
	{
		try
		{
			BulkOperationProcessController.getBulkOperationControllerInstance().
					handleBulkOperationJob(this.csvFileInputStream, this.getJobData(), serviceInformationObject,
							bulkOperationClass);
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
		}
	}
}