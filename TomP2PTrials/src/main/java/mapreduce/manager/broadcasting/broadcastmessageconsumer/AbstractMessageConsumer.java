package mapreduce.manager.broadcasting.broadcastmessageconsumer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.manager.broadcasting.broadcastmessages.IBCMessage;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.PeerAddress;

/**
 * <code>MessageConsumer</code> stores incoming <code>IBCMessage</code> on a queue for future processing
 * 
 * @author ozihler
 *
 */
public abstract class AbstractMessageConsumer implements IMessageConsumer {

	protected static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

	protected BlockingQueue<IBCMessage> bcMessages;
	protected List<Job> jobs;

	private boolean canTake;

	protected AbstractMessageConsumer(BlockingQueue<IBCMessage> bcMessages, List<Job> jobs) {
		this.bcMessages = bcMessages;
		this.jobs = jobs;
	}

	@Override
	public void run() {

		try {
			while (canTake()) {
				logger.info("Before Take message");
				IBCMessage nextMessage = bcMessages.take();
				logger.info("Execute next message: " + nextMessage);
				nextMessage.execute(this);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public AbstractMessageConsumer canTake(boolean canTake) {
		this.canTake = canTake;
		return this;
	}

	@Override
	public boolean canTake() {
		return this.canTake;
	}

	@Override
	public BlockingQueue<IBCMessage> queue() {
		return this.bcMessages;
	}

	@Override
	public List<Job> jobs() {
		return jobs;
	}

	// Dummy implementations
	@Override
	public void handleFinishedJob(Job job) {
	}

	@Override
	public void handleNewExecutorOnline() {

	}

	@Override
	public void handleReceivedJob(Job job) {

	}

	@Override
	public void handleTaskExecutionStatusUpdate(Job job, Task task, TaskResult toUpdate) {

	}

	@Override
	public void handleFinishedProcedure(Job job) {

	}

	@Override
	public void handleFailedJob(Job job) {
	}
}