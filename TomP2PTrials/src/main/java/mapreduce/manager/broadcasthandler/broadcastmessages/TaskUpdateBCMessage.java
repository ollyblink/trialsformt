package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.execution.task.TaskResult;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;
import net.tomp2p.peers.Number160;

public class TaskUpdateBCMessage extends AbstractTaskBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7388714464226222965L;
	protected BCMessageStatus status;
	private Number160 resultHash;

	@Override
	public BCMessageStatus status() {
		return status;
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		TaskResult taskResult = TaskResult.newInstance().sender(sender).status(status).resultHash(resultHash);
		messageConsumer.handleTaskExecutionStatusUpdate(task, taskResult);

	}

	public static TaskUpdateBCMessage newExecutingTaskInstance() {
		return new TaskUpdateBCMessage(BCMessageStatus.EXECUTING_TASK);
	}

	public static TaskUpdateBCMessage newFinishedTaskInstance() {
		return new TaskUpdateBCMessage(BCMessageStatus.FINISHED_TASK);
	}

	public static TaskUpdateBCMessage newFailedTaskInstance() {

		return new TaskUpdateBCMessage(BCMessageStatus.FAILED_TASK);
	}
	// public static TaskUpdateBCMessage newExecutingTaskResultComparisonInstance() {
	// return new TaskUpdateBCMessage(BCMessageStatus.EXECUTING_TASK_COMPARISON);
	// }
	//
	// public static TaskUpdateBCMessage newFinishedTaskResultComparisonInstance() {
	// return new TaskUpdateBCMessage(BCMessageStatus.FINISHED_TASK_COMPARISON);
	// }

	private TaskUpdateBCMessage(BCMessageStatus status) {
		this.status = status;
	}

	public Number160 resultHash() {
		return this.resultHash;
	}

	public TaskUpdateBCMessage resultHash(Number160 resultHash) {
		this.resultHash = resultHash;
		return this;
	}


}
