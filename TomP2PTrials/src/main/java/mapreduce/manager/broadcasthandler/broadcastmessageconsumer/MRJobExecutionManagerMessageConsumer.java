package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.Tasks;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.conditions.ICondition;
import mapreduce.manager.conditions.JobBCMessageUpdateCondition;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutionManagerMessageConsumer extends AbstractMessageConsumer {

	private static final BCMessageStatus[] FINISHED_ALL_TASKS_MESSAGES_TO_REMOVE = { BCMessageStatus.EXECUTING_TASK, BCMessageStatus.FINISHED_TASK };

	private Set<BCMessageStatus> finishedAllTasksMessagesToRemove;
	private JobBCMessageUpdateCondition jobBCMessageUpdateCondition;
	private MRJobExecutionManager jobExecutor;

	private MRJobExecutionManagerMessageConsumer(BlockingQueue<IBCMessage> bcMessages, List<Job> jobs) {
		super(bcMessages, jobs);

		this.finishedAllTasksMessagesToRemove = new HashSet<BCMessageStatus>();
		Collections.addAll(finishedAllTasksMessagesToRemove, FINISHED_ALL_TASKS_MESSAGES_TO_REMOVE);

		this.jobBCMessageUpdateCondition = JobBCMessageUpdateCondition.create();
	}

	public static MRJobExecutionManagerMessageConsumer newInstance(List<Job> jobs) {
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
			logger.info("NEW JOB: " + job.id());
			jobs.add(job);
		}
	}

	@Override
	public void handleTaskExecutionStatusUpdate(Task taskToUpdate, TaskResult toUpdate) {
		synchronized (jobs) {
			for (Job job : jobs) {
				if (job.id().equals(taskToUpdate.jobId())) {
					List<Task> tasks = job.currentProcedure().tasks();
					synchronized (tasks) {
						int taskIndex = tasks.indexOf(taskToUpdate);
						Tasks.updateStati(tasks.get(taskIndex), toUpdate, job.maxNrOfFinishedWorkersPerTask());
					}
				}
			}
		}

	}

	@Override
	public void updateJob(Job job, PeerAddress sender) {
		// if (jobExecutor.dhtConnectionProvider().peerAddress().equals(sender)) { // sent it to myself... Nothing to do
		// return;
		// }
		if (!sender.equals(this.jobExecutor.dhtConnectionProvider().peerAddress())) {
			this.jobExecutor.abortExecution(job);
		}
		logger.info("Sync job");
		this.jobs.set(this.jobs.indexOf(job), job);
		logger.info("Synced job");
		this.updateMessagesFromJobUpdate(job.id());
		logger.info("removed messages for job");
	}

	public void updateMessagesFromJobUpdate(String jobId) {
		this.removeMessagesForJob(jobBCMessageUpdateCondition.jobId(jobId).types(finishedAllTasksMessagesToRemove));
	}

	private void removeMessagesForJob(ICondition<IBCMessage> removeMessageCondition) {
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
	public void handleFinishedJob(Job job) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleFailedJob(Job job) {
		// TODO Auto-generated method stub

	}

}
