package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.computation.standardprocedures.EndReached;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.Tasks;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.conditions.JobBCMessageUpdateCondition;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;

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
		} else { // Job was already received once.
			// TODO: Currently don't know what to do with that information... skip
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
	public void handleFinishedAllTasks(Job job) {
		// if (jobExecutor.dhtConnectionProvider().peerAddress().equals(sender)) { // sent it to myself... Nothing to do
		// return;
		// }
		if (this.jobExecutor.currentlyExecutedJob().equals(job)) {
			this.jobExecutor.abortExecution(job); // finished already... doesn't need to be executed anymore

			// TODO: hmnn... Should I start another job here while we wait for the evaluatiion of this job? I DON'T KNOW... Well let's think this
			// through... Only if it was the last procedure and the job is finished, else if we take the next procedure of the same job, there isn't
			// any data to collect for tasks yet (will be done below)... Sooo will think about it...

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
		String jobProcedureDomain = DomainProvider.INSTANCE.jobProcedureDomain(job);
		List<Task> tasks = job.currentProcedure().tasks();
		List<FuturePut> futurePuts = SyncedCollectionProvider.syncedArrayList();
		for (Task task : tasks) {
			List<String> finalDataLocationDomains = task.finalDataLocationDomains();
			for (String finalDataLocationDomain : finalDataLocationDomains) {
				futurePuts.add(this.jobExecutor.dhtConnectionProvider().add(task.id(), finalDataLocationDomain, jobProcedureDomain, false));
			}
		}
		Futures.whenAllSuccess(futurePuts).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					// well... check if there is actually any procedure left... Else job is finished...
					ProcedureInformation subsequentProcedure = job.subsequentProcedure();
					if (subsequentProcedure.procedure().getClass().getSimpleName().equals(EndReached.class.getSimpleName())) {
						// Finished job :)
						jobExecutor.dhtConnectionProvider().broadcastFinishedJob(job);
						jobs.remove(job);
					} else {
						// Next procedure!!
						job.incrementCurrentProcedureIndex();
						Job nextJob = job;
						if (!jobs.get(0).equals(nextJob)) {
							nextJob = jobs.get(0);// Sorry little job... but maybe a new, more prioritized job needs to be executed asap so you need
													// to step back and wait a little...
						}
						jobExecutor.executeJob(nextJob);
					}
				} else {
					// TODO Well... something has to be done instead... am I right?
				}
			}

		});
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
