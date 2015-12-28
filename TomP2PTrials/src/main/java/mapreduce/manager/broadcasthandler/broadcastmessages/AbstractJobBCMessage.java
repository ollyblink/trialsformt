package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.execution.job.Job;

/**
 * Any message updating a job should extend this class
 * 
 * @author ozihler
 *
 */
public abstract class AbstractJobBCMessage extends AbstractBCMessage implements IJobBCMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5982534238858537033L;
	protected Job job;

	public Job job() {
		return this.job;
	}

	public AbstractJobBCMessage job(Job job) {
		this.job = job;
		return this;
	}

	@Override
	public String jobId() {
		return job.id();
	}

	@Override
	public String toString() {
		return super.toString() + "AbstractJobBCMessage [job=" + job + "]";
	}

	// @Override
	// public String toString() {
	// return super.toString() + "AbstractTaskBCMessage [task=" + task + "]";
	// }

}