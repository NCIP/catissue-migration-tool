package edu.wustl.bulkoperator.jobmanager;


public interface BulkJobInterface extends Runnable
{
	/**
	 * Gets the job data.
	 *
	 * @return the job data
	 */
	public JobData getJobData();

	/**
	 * Gets the job name.
	 * @return the job name
	 */
	public String getJobName();

	/**
	 * Gets the job started by.
	 *
	 * @return the job started by
	 */
	public String getJobStartedBy();



	/**
	 * Do job.
	 */
	public abstract void doJob();
}
