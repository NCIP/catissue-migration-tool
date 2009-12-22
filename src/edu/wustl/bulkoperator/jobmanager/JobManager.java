
package edu.wustl.bulkoperator.jobmanager;


// TODO: Auto-generated Javadoc
/**
 * The JobManager class will manages all the job/threads.
 * All the jobs will get initiated by the job manager this is a singleton class
 *
 * @author nitesh_marwaha
 */
final public class JobManager
{
	/** The job manager instance. */
	private static JobManager jobMgrInstance;

	/**
	 * Instantiates a new job manager.
	 */
	private JobManager()
	{
	}

	/**
	 * Gets the single instance of JobManager.
	 *
	 * @return single instance of JobManager
	 */
	public static JobManager getInstance()
	{
		if (jobMgrInstance == null)
		{
			jobMgrInstance = new JobManager();
		}
		return jobMgrInstance;
	}

	/**
	 * Adds the job.
	 *
	 * @param job the job
	 */
	public synchronized void addJob(final Job job)
	{
		final Thread jobThread = new Thread(job);
		jobThread.start();
	}
}