package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCStatusType;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

/**
 * <code>MessageConsumer</code> stores incoming <code>IBCMessage</code> on a queue for future processing
 * 
 * @author ozihler
 *
 */
public abstract class AbstractMessageConsumer implements IMessageConsumer {

	protected static Logger logger = LoggerFactory.getLogger(AbstractMessageConsumer.class);

	/* These two blocking queues are used for seemless interaction between classes */
	protected BlockingQueue<IBCMessage> bcMessages;
	protected BlockingQueue<Job> jobs;

	private boolean canTake;
	/** Used to signal e.g. the job executor that currently this message consumer is busy */
	private boolean isBusy;

	protected AbstractMessageConsumer(BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		this.bcMessages = bcMessages;
		this.jobs = jobs;
	}

	@Override
	public void run() {
		try {
			while (canTake()) {
				final IBCMessage nextMessage = bcMessages.take();
				handleBCMessage(nextMessage);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	protected abstract void handleBCMessage(IBCMessage nextMessage);

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
	public BlockingQueue<Job> jobs() {
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
	public void handleReceivedJob(Job job) {
	}

	@Override
	public void handleFinishedJob(String jobId, String jobSubmitterId) {
	}

	@Override
	public void handleFinishedAllTasks(String jobId, Collection<Task> tasks, PeerAddress sender) {

	}

	@Override
	public void handleNewExecutorOnline() {

	}

	@Override
	public void handleTaskExecutionStatusUpdate(String jobId, String taskId, Tuple<PeerAddress, BCStatusType> toUpdate) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleFinishedTaskComparion(String jobId, String taskId, Tuple<PeerAddress, Integer> finalDataLocation) {
		// TODO Auto-generated method stub

	}
}