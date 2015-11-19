package firstdesignidea.execution.broadcasthandler.broadcastmessages;

import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.JobStatus;
import firstdesignidea.server.MRJobExecutor;

public class DistributedJobBCMessage implements IBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2792435155108555907L;
	private Job job;

	private DistributedJobBCMessage() {
	}

	public static DistributedJobBCMessage newDistributedTaskBCMessage() {
		return new DistributedJobBCMessage();
	}

	@Override
	public JobStatus status() {
		return JobStatus.DISTRIBUTED_JOB;
	}

	public Job job() {
		return this.job;
	}

	public DistributedJobBCMessage job(Job job) {
		this.job = job;
		return this;
	}

	@Override
	public void execute(MRJobExecutor mrJobExecutor) {
		mrJobExecutor.executeJob(job);

	}
}
