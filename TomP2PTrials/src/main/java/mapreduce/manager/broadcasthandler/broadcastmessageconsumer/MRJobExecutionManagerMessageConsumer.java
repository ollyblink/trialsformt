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
import mapreduce.manager.conditions.JobBCMessageUpdateCondition;

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
		if (!jobs.contains(job)) { // Job was not yet received
			jobs.add(job);
			synchronized (jobs) {
				Collections.sort(jobs);
			}
			if (jobExecutor.currentlyExecutedJob() == null) { // No job is executed atm
				jobExecutor.executeJob(jobs.get(0)); // Execute job with highest priority
			} else {// Some job is executed atm
				if (!jobExecutor.currentlyExecutedJob().equals(jobs.get(0))) { // The job currently executing is not the one with highest priority
																				// anymore
					jobExecutor.abortExecution(job); // Abort the currently executing job (meaning, interrupt until later resume... This finishes the
														// task executing atm
					jobExecutor.executeJob(jobs.get(0));
				} else {
					// Do nothing and continue executing
				}
			}
		} else { // Job was already received once. This is either an update or a new submission because another task for the job was added
			Job containedJob = jobs.get(jobs.indexOf(job)); // get that job from own list and see what's different
			// Was a task added for execution?
			containedJob.currentProcedure().tasks();
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
						if (taskIndex < 0) {// Received a new task currently not yet assigned
							tasks.add(taskToUpdate);
							taskIndex = tasks.size() - 1;
						}
						Tasks.updateStati(tasks.get(taskIndex), toUpdate, job.maxNrOfFinishedWorkersPerTask());
						break;
					}
				}
			}
		}
	}

	@Override
	public void handleFinishedAllTasks(Job job, String sender) {
		// if (jobExecutor.dhtConnectionProvider().peerAddress().equals(sender)) { // sent it to myself... Nothing to do
		// return;
		// }
		if (!sender.equals(this.jobExecutor.dhtConnectionProvider().owner())) {
			this.jobExecutor.abortExecution(job);
		}
		logger.info("Sync job");
		this.jobs.set(this.jobs.indexOf(job), job);
		logger.info("Synced job");
		jobBCMessageUpdateCondition.jobId(job.id()).types(finishedAllTasksMessagesToRemove);
		synchronized (this.bcMessages) {
			BlockingQueue<IBCMessage> tmp = new PriorityBlockingQueue<IBCMessage>();
			for (IBCMessage t : this.bcMessages) {
				if (jobBCMessageUpdateCondition.metBy(t)) {
					tmp.add(t);
				}
			}
			this.bcMessages = tmp;
		}
		logger.info("removed messages for job");
		logger.info("job current tasks for current procedure: " + job.currentProcedure().tasks().get(0).executingPeers().keySet());
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
