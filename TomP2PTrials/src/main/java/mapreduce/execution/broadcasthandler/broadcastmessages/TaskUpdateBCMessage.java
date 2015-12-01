package mapreduce.execution.broadcasthandler.broadcastmessages;

import mapreduce.execution.broadcasthandler.messageconsumer.IMessageConsumer;
import net.tomp2p.peers.PeerAddress;

public class TaskUpdateBCMessage extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7388714464226222965L;
	private String taskId;
	private String jobId;
	private BCStatusType status;

	@Override
	public BCStatusType status() {
		return status;
	}

	@Override
	public void execute(IMessageConsumer messageConsumer) {
		messageConsumer.handleTaskExecutionStatusUpdate(jobId, taskId, sender, status());
	}

	public static TaskUpdateBCMessage newExecutingTaskInstance() {
		return new TaskUpdateBCMessage(BCStatusType.EXECUTING_TASK);
	}

	public static TaskUpdateBCMessage newFinishedTaskInstance() {
		return new TaskUpdateBCMessage(BCStatusType.FINISHED_TASK);
	}
	
	public static TaskUpdateBCMessage newExecutingCompareTaskResults() {
		return new TaskUpdateBCMessage(BCStatusType.EXECUTING_TASK_COMPARISON);
	}
	
	public static TaskUpdateBCMessage newFinishedCompareTaskResults() {
		return new TaskUpdateBCMessage(BCStatusType.FINISHED_TASK_COMPARISON);
	}

	private TaskUpdateBCMessage(BCStatusType status) {
		this.status = status;
	}

	public TaskUpdateBCMessage taskId(String taskId) {
		this.taskId = taskId;
		return this;
	}

	public TaskUpdateBCMessage jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	@Override
	public TaskUpdateBCMessage sender(PeerAddress peerAddress) {
		return (TaskUpdateBCMessage) super.sender(peerAddress);
	}

	@Override
	public String toString() {
		return "ExecuteOrFinishedTaskMessage [taskId=" + taskId + ", jobId=" + jobId + ", status=" + status + "]";
	}


}
