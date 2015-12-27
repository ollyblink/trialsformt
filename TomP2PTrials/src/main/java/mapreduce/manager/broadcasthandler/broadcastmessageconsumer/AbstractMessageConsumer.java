package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
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
	/** Used to signal e.g. the job executor that currently this message consumer is busy */
	private boolean isBusy;

	protected AbstractMessageConsumer(BlockingQueue<IBCMessage> bcMessages, List<Job> jobs) {
		this.bcMessages = bcMessages;
		this.jobs = jobs;
	}

	@Override
	public void run() {
		try {
			while (canTake()) {
				
				final IBCMessage nextMessage = bcMessages.take();
				logger.info("Next message: " +nextMessage);
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

	public AbstractMessageConsumer isBusy(boolean isBusy) {
		this.isBusy = isBusy;
		return this;
	}

	public boolean isBusy() {
		return this.isBusy;
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
	public void handleTaskExecutionStatusUpdate(Task task, TaskResult toUpdate) {

	}

	@Override
	public void handleFinishedAllTasks(Job job) {

	}
}