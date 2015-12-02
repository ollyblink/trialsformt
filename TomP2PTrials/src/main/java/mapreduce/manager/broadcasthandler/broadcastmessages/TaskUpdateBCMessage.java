package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class TaskUpdateBCMessage extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7388714464226222965L;
	protected String taskId;
	protected String jobId;
	protected BCStatusType status;

	@Override
	public BCStatusType status() {
		return status;
	}

	@Override
	public void execute(IMessageConsumer messageConsumer) {
		messageConsumer.handleTaskExecutionStatusUpdate(jobId, taskId, Tuple.newInstance(sender, status()));
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

	protected TaskUpdateBCMessage(BCStatusType status) {
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
