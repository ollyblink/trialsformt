package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class TaskUpdateBCMessage extends AbstractTaskBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7388714464226222965L;
	protected BCMessageStatus status;

	@Override
	public BCMessageStatus status() {
		return status;
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleTaskExecutionStatusUpdate(task, Tuple.create(sender, status()));
	}

	public static TaskUpdateBCMessage newExecutingTaskInstance() {
		return new TaskUpdateBCMessage(BCMessageStatus.EXECUTING_TASK);
	}

	public static TaskUpdateBCMessage newFinishedTaskInstance() {
		return new TaskUpdateBCMessage(BCMessageStatus.FINISHED_TASK);
	}

	public static TaskUpdateBCMessage newExecutingCompareTaskResultsInstance() {
		return new TaskUpdateBCMessage(BCMessageStatus.EXECUTING_TASK_COMPARISON);
	}

	public static TaskUpdateBCMessage newFinishedCompareTaskResultsInstance() {
		return new TaskUpdateBCMessage(BCMessageStatus.FINISHED_TASK_COMPARISON);
	}

	protected TaskUpdateBCMessage(BCMessageStatus status) {
		this.status = status;
	}

}
