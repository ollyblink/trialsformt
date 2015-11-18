package firstdesignidea.execution.broadcasthandler.broadcastmessages;

import firstdesignidea.execution.jobtask.JobStatus;

public class DistributedTaskBCMessage implements IBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2792435155108555907L;
	private String taskId;
	private String jobId;

	private DistributedTaskBCMessage() {
	}

	public static DistributedTaskBCMessage newDistributedTaskBCMessage() {
		return new DistributedTaskBCMessage();
	}

	@Override
	public JobStatus status() {
		return JobStatus.DISTRIBUTED_TASKS;
	}

	public DistributedTaskBCMessage taskId(String taskId) {
		this.taskId = taskId;
		return this;
	}

	public String taskId() {
		return taskId;
	}

	public DistributedTaskBCMessage jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	public String jobId() {
		return jobId;
	}

}
