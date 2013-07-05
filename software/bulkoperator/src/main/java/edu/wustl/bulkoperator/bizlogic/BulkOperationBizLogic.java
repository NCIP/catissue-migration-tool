package edu.wustl.bulkoperator.bizlogic;

import java.io.InputStream;

import edu.wustl.bulkoperator.client.BulkOperatorJob;
import edu.wustl.bulkoperator.dao.BulkOperationDao;
import edu.wustl.bulkoperator.jobmanager.JobDetails;
import edu.wustl.bulkoperator.jobmanager.JobManager;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.BulkOperationTemplate;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.bizlogic.DefaultBizLogic;
import edu.wustl.common.exception.ApplicationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.logger.Logger;


public class BulkOperationBizLogic extends DefaultBizLogic {
	/**
	 * Logger added for Specimen class.
	 */
	private static final Logger logger = Logger.getCommonLogger(BulkOperationBizLogic.class);
	
	public JobDetails getJobDetails(String jobId)
		throws BulkOperationException, ApplicationException {
		return (JobDetails)retrieve(JobDetails.class.getName(), Long.valueOf(jobId));
	}
	
	public Long submitJob(SessionDataBean sessionDataBean, String templateName, InputStream csvFileIn) 
	throws BulkOperationException {
		Long jobId = null;
		
		try {
			BulkOperationTemplate template = BulkOperationDao.getTemplateDetails(templateName);
			BulkOperation bulkOperation = BulkOperation.fromXml(template.getXmlTemplate());
			jobId = startBulkOperation(sessionDataBean, bulkOperation, template.getOperationName(), csvFileIn);
		} catch (Exception e) {
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.operation.generic.issues");
			throw new BulkOperationException(errorKey, e, "");
		}
		
		return jobId;
	}

	
	private Long startBulkOperation(SessionDataBean sessionDataBean, BulkOperation bulkOperation,
			String operationName, InputStream csvFileInputStream) throws BulkOperationException {
		BulkOperatorJob job = new BulkOperatorJob(sessionDataBean, operationName, bulkOperation, csvFileInputStream);

		JobManager.getInstance().addJob(job);
		
		//
		// TODO: VP: Need to remove this busy-wait style loop
		//
		while(job.getJobData() == null) {
			logger.debug("Job not started yet !!!");
		}
		return job.getJobData().getJobID();
	}
}