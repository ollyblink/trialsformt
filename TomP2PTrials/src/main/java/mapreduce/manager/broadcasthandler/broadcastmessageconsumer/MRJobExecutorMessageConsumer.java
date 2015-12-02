package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import com.google.common.reflect.TypeToken.TypeSet;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCStatusType;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutorMessageConsumer extends AbstractMessageConsumer {

	private MRJobExecutionManager jobExecutor;

	private MRJobExecutorMessageConsumer(BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		super(bcMessages, jobs);

	}

	public static MRJobExecutorMessageConsumer newInstance(BlockingQueue<Job> jobs) {
		return new MRJobExecutorMessageConsumer(new PriorityBlockingQueue<IBCMessage>(), jobs);
	}

	/**
	 * Use this for interrupting execution (canExecute(false))
	 * 
	 * @param mrJobExecutor
	 * @return
	 */
	public MRJobExecutorMessageConsumer jobExecutor(MRJobExecutionManager mrJobExecutor) {
		this.jobExecutor = mrJobExecutor;
		return this;
	}

	@Override
	public MRJobExecutorMessageConsumer canTake(boolean canTake) {
		return (MRJobExecutorMessageConsumer) super.canTake(canTake);
	}

	@Override
	protected void handleBCMessage(IBCMessage message) {
		message.execute(this);
	}

	@Override
	public void handleReceivedJob(Job job) {
		logger.warn("Adding new job " + job.id());
		if (!jobs.contains(job)) {
			jobs.add(job);
		}
	}

	@Override
	public void handleTaskExecutionStatusUpdate(String jobId, String taskId, Tuple<PeerAddress, BCStatusType> toUpdate) {
		for (Job job : jobs) {
			if (job.id().equals(jobId)) {
				job.updateTaskExecutionStatus(taskId, toUpdate);
			}
		}
	}

	@Override
	public void handleFinishedAllTasks(String jobId, Collection<Task> tasks, PeerAddress sender) {
		if (jobExecutor.dhtConnectionProvider().peerAddress().equals(sender)) { // sent it to myself... Nothing to do
			return;
		}
		logger.info("This executor has its task execution aborted!");

		jobExecutor.abortTaskExecution();
		syncTasks(jobId, tasks);
	}

	private void syncTasks(String jobId, Collection<Task> tasks) {
		for (Job job : jobs) {
			if (job.id().equals(jobId)) {
				this.isBusy(true);
				job.synchronizeFinishedTasksStati(tasks);
				removeMessagesFromJobWithStati(jobId);
				this.isBusy(false);
			}
		}
	}

	@Override
	public void handleFinishedAllTaskComparisons(String jobId, Collection<Task> tasks, PeerAddress sender) {
		if (jobExecutor.dhtConnectionProvider().peerAddress().equals(sender)) { // sent it to myself... Nothing to do
			return;
		}
		logger.info("This executor has its task comparisons aborted!");
		jobExecutor.abortTaskComparison();
		syncTasks(jobId, tasks);
	}

	public void removeMessagesFromJobWithStati(String jobId, BCStatusType... stati) {
		Set<BCStatusType> typesSet = new HashSet<BCStatusType>();
		Collections.addAll(typesSet, stati);
		synchronized (bcMessages) {
			BlockingQueue<IBCMessage> remainingBCMessages = new PriorityBlockingQueue<IBCMessage>();
			for (IBCMessage message : bcMessages) {
				if (!message.jobId().equals(jobId) && !typesSet.contains(message.status())) {
					remainingBCMessages.add(message);
					logger.warn("Kept message: " + message);
				} else {
					logger.warn("Removed message: " + message);
				}
			}
			this.bcMessages = remainingBCMessages;
		}
	}

	@Override
	public void handleFinishedTaskComparion(String jobId, String taskId, Tuple<PeerAddress, Integer> finalDataLocation) {
		for (Job job : jobs) {
			if (job.id().equals(jobId)) {
				job.updateTaskFinalDataLocation(taskId, finalDataLocation);
			}
		}
	}

}
