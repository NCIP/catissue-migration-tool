/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue-migration-tool/LICENSE.txt for details.
 */

package edu.wustl.bulkoperator.client;

import edu.wustl.bulkoperator.BulkOperator;
import edu.wustl.bulkoperator.DataList;
import edu.wustl.bulkoperator.jobmanager.Job;
import edu.wustl.bulkoperator.jobmanager.JobStatusListener;
import edu.wustl.common.util.logger.Logger;

public class BulkOperatorJob extends Job
{
	/**
	 * logger instance of the class.
	 */
	private final static Logger logger = Logger.getCommonLogger(BulkOperatorJob.class);
	private BulkOperator bulkOperator = null;
	private String loginName = null;
	private String password = null;
	private String className = null;
	private DataList dataList = null;

	public BulkOperatorJob(String operationName, String loginName, String password, String userId,
			BulkOperator bulkOperator, DataList dataList, String className,
			JobStatusListener jobStatusListener)
	{
		super(operationName, userId, jobStatusListener);
		this.bulkOperator = bulkOperator;
		this.loginName = loginName;
		this.password = password;
		this.className = className;
		this.dataList = dataList;
	}

	@Override
	public void doJob()
	{
		try
		{
			bulkOperator.startProcess(getJobName(), this.loginName, this.password, getJobStartedBy(),
					this.dataList, this.className, this.getJobData());
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
		}
	}
}