
package edu.wustl.bulkoperator.client;

import java.io.InputStream;

import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.controller.BulkOperationProcessController;
import edu.wustl.bulkoperator.jobmanager.AbstractJob;
import edu.wustl.bulkoperator.jobmanager.JobStatusListener;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.common.beans.SessionDataBean;
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
	private SessionDataBean sessionDataBean = null;

	public BulkOperatorJob(String operationName, SessionDataBean sessionDataBean,
			InputStream csvFileInputStream, JobStatusListener jobStatusListener,
			AppServiceInformationObject serviceInformationObject, BulkOperationClass bulkOperationClass)
	{
		super(operationName, String.valueOf(sessionDataBean.getUserId()), jobStatusListener);
		this.serviceInformationObject =serviceInformationObject;
		this.csvFileInputStream = csvFileInputStream;
		this.bulkOperationClass = bulkOperationClass;
		this.sessionDataBean = sessionDataBean;
	}

	@Override
	public void doJob()
	{
		try
		{
			BulkOperationProcessController.getBulkOperationControllerInstance().
					handleBulkOperationJob(this.csvFileInputStream, this.getJobData(), serviceInformationObject,
							bulkOperationClass,sessionDataBean);
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			exp.printStackTrace();
		}
	}
}