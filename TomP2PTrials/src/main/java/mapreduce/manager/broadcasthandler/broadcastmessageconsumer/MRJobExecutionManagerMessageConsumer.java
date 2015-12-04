package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.conditions.ICondition;
import mapreduce.manager.conditions.JobBCMessageUpdateCondition;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutionManagerMessageConsumer extends AbstractMessageConsumer {

	private static final BCMessageStatus[] FINISHED_ALL_TASKS_MESSAGES_TO_REMOVE = { BCMessageStatus.EXECUTING_TASK, BCMessageStatus.FINISHED_TASK };
	private static final BCMessageStatus[] FINISHED_ALL_TASK_COMPARISONS_MESSAGES_TO_REMOVE = { BCMessageStatus.EXECUTING_TASK,
			BCMessageStatus.FINISHED_TASK };
	private Set<BCMessageStatus> finishedAllTasksMessagesToRemove;
	private Set<BCMessageStatus> finishedAllTaskComparisonsMessagesToRemove;
	private JobBCMessageUpdateCondition jobBCMessageUpdateCondition;
	private MRJobExecutionManager jobExecutor;

	private MRJobExecutionManagerMessageConsumer(BlockingQueue<IBCMessage> bcMessages, BlockingQueue<Job> jobs) {
		super(bcMessages, jobs);

		this.finishedAllTasksMessagesToRemove = new HashSet<BCMessageStatus>();
		Collections.addAll(finishedAllTasksMessagesToRemove, FINISHED_ALL_TASKS_MESSAGES_TO_REMOVE);

		this.finishedAllTaskComparisonsMessagesToRemove = new HashSet<BCMessageStatus>();
		Collections.addAll(finishedAllTaskComparisonsMessagesToRemove, FINISHED_ALL_TASK_COMPARISONS_MESSAGES_TO_REMOVE);

		this.jobBCMessageUpdateCondition = JobBCMessageUpdateCondition.newInstance();
	}

	public static MRJobExecutionManagerMessageConsumer newInstance(BlockingQueue<Job> jobs) {
		return new MRJobExecutionManagerMessageConsumer(new PriorityBlockingQueue<IBCMessage>(), jobs);
	}

	/**
	 * Use this for interrupting execution (canExecute(false))
	 * 
	 * @param mrJobExecutor
	 * @return
	 */
	public MRJobExecutionManagerMessageConsumer jobExecutor(MRJobExecutionManager mrJobExecutor) {
		this.jobExecutor = mrJobExecutor;
		return this;
	}

	@Override
	public MRJobExecutionManagerMessageConsumer canTake(boolean canTake) {
		return (MRJobExecutionManagerMessageConsumer) super.canTake(canTake);
	}

	@Override
	public void handleReceivedJob(Job job) {
		if (!jobs.contains(job)) {
			jobs.add(job);
		}
	}

	@Override
	public void handleTaskExecutionStatusUpdate(Task task, Tuple<PeerAddress, BCMessageStatus> toUpdate) {
		for (Job job : jobs) {
			if (job.id().equals(task.jobId())) {
				job.updateTaskExecutionStatus(task.id(), toUpdate);
			}
		}
	}

	@Override
	public void handleFinishedTaskComparion(Task task) {
		for (Job job : jobs) {
			if (job.id().equals(task.jobId())) {
				job.updateTaskFinalDataLocation(task);
			}
		}
	}

	@Override
	public void updateJob(Job job, BCMessageStatus status, PeerAddress sender) {
		if (jobExecutor.dhtConnectionProvider().peerAddress().equals(sender)) { // sent it to myself... Nothing to do
			return;
		}
		this.isBusy(true);
		this.jobExecutor.abortExecution(status);
		this.syncJob(job);
		this.updateMessagesFromJobUpdate(job.id(), status);
		this.isBusy(false);
	}

	private void syncJob(Job job) {
		List<Job> jobsList = Collections.synchronizedList(new ArrayList<Job>(jobs));
		synchronized (jobsList) {
			jobsList.set(jobsList.indexOf(job), job);
			this.jobs.clear();
			this.jobs.addAll(jobsList);
		}
	}

	public void updateMessagesFromJobUpdate(String jobId, BCMessageStatus status) {
		jobBCMessageUpdateCondition.jobId(jobId);
		switch (status) {
		case FINISHED_ALL_TASKS:
			jobBCMessageUpdateCondition.types(finishedAllTasksMessagesToRemove);
			break;
		case FINISHED_ALL_TASK_COMPARIONS:
			jobBCMessageUpdateCondition.types(finishedAllTaskComparisonsMessagesToRemove);
			break;
		default:
			break;
		}
		this.removeMessagesFromJob(jobBCMessageUpdateCondition);
	}

	private void removeMessagesFromJob(ICondition<IBCMessage> removeMessageCondition) {
		synchronized (this.bcMessages) {
			BlockingQueue<IBCMessage> tmp = new PriorityBlockingQueue<IBCMessage>();
			for (IBCMessage t : this.bcMessages) {
				if (removeMessageCondition.metBy(t)) {
					tmp.add(t);
				}
			}
			this.bcMessages = tmp;
		}
	}

	@Override
	public void handleFinishedJob(Job job, String jobSubmitterId) {
		// TODO Auto-generated method stub

	}

}
