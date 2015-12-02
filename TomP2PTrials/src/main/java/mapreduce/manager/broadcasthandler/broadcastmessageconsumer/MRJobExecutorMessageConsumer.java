package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

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
	public void handleFinishedTaskComparion(String jobId, String taskId, Tuple<PeerAddress, Integer> finalDataLocation) {
		for (Job job : jobs) {
			if (job.id().equals(jobId)) {
				job.updateTaskFinalDataLocation(taskId, finalDataLocation);
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
		for (Job job : jobs) {
			if (job.id().equals(jobId)) {
				this.isBusy(true);
				job.synchronizeFinishedTasksStati(tasks);

				removeRemainingExecutionMessagesForThisTask(jobId);

				// Only printing
				BlockingQueue<Task> ts = job.tasks(job.currentProcedureIndex());
				for (Task t : ts) {
					logger.info("Task " + t.id());
					for (PeerAddress pAddress : t.allAssignedPeers()) {
						logger.info(pAddress.inetAddress() + ":" + pAddress.tcpPort() + ": " + t.statiForPeer(pAddress));
					}
				}
				this.isBusy(false);
			}
		}
	}

	public void removeRemainingExecutionMessagesForThisTask(String jobId) {
		synchronized (bcMessages) {
			BlockingQueue<IBCMessage> remainingBCMessages = new PriorityBlockingQueue<IBCMessage>();

			for (IBCMessage message : bcMessages) {
				if (!message.jobId().equals(jobId)) {
					remainingBCMessages.add(message);
					logger.warn("Kept message: " + message);
				} else {
					logger.warn("Removed message: " + message);
				}
			}
			this.bcMessages = remainingBCMessages;
		}
	}

}
