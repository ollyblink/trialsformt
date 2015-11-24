package mapreduce.execution.broadcasthandler.broadcastmessages;

import mapreduce.execution.broadcasthandler.MessageConsumer;
import mapreduce.server.MRJobExecutor;
import net.tomp2p.peers.PeerAddress;

public class ExecuteOrFinishedTaskMessage extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7388714464226222965L;
	private String taskId;
	private String jobId;

	private JobStatus status;

	@Override
	public JobStatus status() {
		return status;
	}

	@Override
	public void execute(MessageConsumer messageConsumer) {
		messageConsumer.updateTask(jobId, taskId, sender, status());
	}

	public static ExecuteOrFinishedTaskMessage newTaskAssignedBCMessage() {
		return new ExecuteOrFinishedTaskMessage(JobStatus.EXECUTING_TASK);
	}

	public static ExecuteOrFinishedTaskMessage newFinishedTaskBCMessage() {
		return new ExecuteOrFinishedTaskMessage(JobStatus.FINISHED_TASK);
	}

	private ExecuteOrFinishedTaskMessage(JobStatus status) {
		this.status = status;
	}


	public ExecuteOrFinishedTaskMessage taskId(String taskId) {
		this.taskId = taskId;
		return this;
	}

	public ExecuteOrFinishedTaskMessage jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}
	

	@Override
	public ExecuteOrFinishedTaskMessage sender(PeerAddress peerAddress) {
		return (ExecuteOrFinishedTaskMessage)super.sender(peerAddress); 
	}
}