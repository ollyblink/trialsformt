package mapreduce.execution.broadcasthandler.broadcastmessages;

import mapreduce.execution.jobtask.JobStatus;
import mapreduce.server.MRJobExecutor;

public class FinishedJobBCMessage extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7088496407737205759L;
	private String jobId;

	@Override
	public JobStatus status() {
		return JobStatus.FINISHED_JOB;
	}

	@Override
	public void execute(MessageConsumer messageConsumer) {
		messageConsumer.handleFinishedJob(jobId);
	}

	public static FinishedJobBCMessage newFinishedJobBCMessage() {
		return new FinishedJobBCMessage();
	}

	public IBCMessage jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

}
