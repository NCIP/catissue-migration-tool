package edu.wustl.common.jobmanager;


// TODO: Auto-generated Javadoc
/**
 * The Interface JobStatusListener.
 *
 * @author nitesh_marwaha
 */
public interface JobStatusListener
{

	/**
	 * Invoked when job status is created.
	 *
	 * @param jobData the job data
	 */
	void jobStatusCreated(JobData jobData);

	/**
	 * Invoked when job status update occurs.
	 *
	 * @param jobData the job data
	 */
	void jobStatusUpdated(JobData jobData);
}
