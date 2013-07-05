
package edu.wustl.bulkoperator.jobmanager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


final public class JobManager
{
	/** The job manager instance. */
	private static JobManager jobMgrInstance;

	private ExecutorService executor = Executors.newCachedThreadPool();
	
	/**
	 * Instantiates a new job manager.
	 */
	private JobManager(){}

	/**
	 * Gets the single instance of JobManager.
	 *
	 * @return single instance of JobManager
	 */
	public synchronized static JobManager getInstance() {
		if (jobMgrInstance == null) {
			jobMgrInstance = new JobManager();
		}
		return jobMgrInstance;
	}

	/**
	 * Adds the job.
	 *
	 * @param job the job
	 */
	public void addJob(final BulkJobInterface job)	{
		executor.execute(job);
	}
}