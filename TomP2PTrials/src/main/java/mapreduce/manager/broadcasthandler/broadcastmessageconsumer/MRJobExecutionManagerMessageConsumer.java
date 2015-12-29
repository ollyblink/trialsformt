package mapreduce.manager.broadcasthandler.broadcastmessageconsumer;

import java.io.IOException;
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
import mapreduce.manager.broadcasthandler.broadcastmessages.DistributedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.FinishedJobBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.broadcasthandler.messageconsumer.MessageConsumerTestSuite;
import mapreduce.manager.conditions.JobBCMessageUpdateCondition;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class MRJobExecutionManagerMessageConsumer extends AbstractMessageConsumer {

	private static final BCMessageStatus[] FINISHED_ALL_TASKS_MESSAGES_TO_REMOVE = { BCMessageStatus.EXECUTING_TASK, BCMessageStatus.FINISHED_TASK,
			BCMessageStatus.FINISHED_PROCEDURE };

	private Set<BCMessageStatus> finishedAllTasksMessagesToRemove;
	private JobBCMessageUpdateCondition jobBCMessageUpdateCondition;
	private MRJobExecutionManager jobExecutor;

	private MRJobExecutionManagerMessageConsumer(BlockingQueue<IBCMessage> bcMessages, List<Job> jobs) {
		super(bcMessages, jobs);

		this.finishedAllTasksMessagesToRemove = new HashSet<BCMessageStatus>();
		Collections.addAll(finishedAllTasksMessagesToRemove, FINISHED_ALL_TASKS_MESSAGES_TO_REMOVE);

		this.jobBCMessageUpdateCondition = JobBCMessageUpdateCondition.create();
	}

	public static MRJobExecutionManagerMessageConsumer newInstance() {
		return new MRJobExecutionManagerMessageConsumer(new PriorityBlockingQueue<IBCMessage>(), SyncedCollectionProvider.syncedArrayList());
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
		logger.info("Received job: " + job);

		if (!jobs.contains(job)) { // Job was not yet received
			jobs.add(job);
			logger.info("Added new job");
		} else { // Job was already received once. So probably the procedure changed
			// if (!sender.equals(jobExecutor.id())) {
			int index = jobs.indexOf(job);
			logger.info("Index: " + index);
			Job job2 = jobs.get(index);
			logger.info("job2.currentProcedureIndex() == job.currentProcedureIndex()" + job2.currentProcedure() + " == " + job.currentProcedure()
					+ " ? " + (job2.currentProcedure().equals(job.currentProcedure())));
			if (job2.currentProcedure().equals(job.currentProcedure())) { // next procedure
				jobs.set(index, job); // replaced
				logger.info("replaced old job with new version as the procedure index increased: from " + job2.currentProcedure() + " to "
						+ job.currentProcedure());
				// }
			}
		}
		synchronized (jobs) {
			Collections.sort(jobs);
		}
		logger.info("Jobs: " + jobs);
		if (jobExecutor.currentlyExecutedJob() != null && !jobExecutor.currentlyExecutedJob().equals(jobs.get(0))) { // No job is executed atm

			logger.info("jobExecutor.currentlyExecutedJob() != null");
			// The job currently executing is not the one with highest priority
			// anymore
			jobExecutor.isExecutionAborted(true); // Abort the currently executing job (meaning, interrupt until later resume... This
													// finishes the
			// task executing atm

		}
		jobExecutor.execute(jobs.get(0));

	}

	@Override
	public void handleTaskExecutionStatusUpdate(Task taskToUpdate, TaskResult toUpdate) {

		synchronized (jobs) {
			logger.info("Looking for job in " + jobs);
			for (Job job : jobs) {
				logger.info(job.id() + ".equals(" + taskToUpdate.jobId() + ")" + job.id().equals(taskToUpdate.jobId()));
				if (job.id().equals(taskToUpdate.jobId())) {
					logger.info("found job: " + job);
					List<Task> tasks = job.subsequentProcedure().tasks();
					logger.info("found tasks: " + tasks);
					synchronized (tasks) {
						int taskIndex = tasks.indexOf(taskToUpdate);
						if (taskIndex < 0) {// Received a new task currently not yet assigned
							tasks.add(taskToUpdate);
							taskIndex = tasks.size() - 1;
						}
						logger.info("Updating task: " + tasks.get(taskIndex) + " with " + toUpdate);
						Tasks.updateStati(tasks.get(taskIndex), toUpdate, job.maxNrOfFinishedWorkersPerTask());
						break;
					}
				}
			}
		}
	}

	@Override
	public void handleFinishedProcedure(Job job) {
		logger.info("handleFinishedProcedure " + job);
		// if (jobExecutor.dhtConnectionProvider().peerAddress().equals(sender)) { // sent it to myself... Nothing to do
		// return;
		// }
		if (this.jobExecutor.currentlyExecutedJob() != null && this.jobExecutor.currentlyExecutedJob().equals(job)) {
			jobExecutor.isExecutionAborted(true); // finished already... doesn't need to be executed anymore

			// TODO: hmnn... Should I start another job here while we wait for the evaluatiion of this job? I DON'T KNOW... Well let's think this
			// through... Only if it was the last procedure and the job is finished, else if we take the next procedure of the same job, there isn't
			// any data to collect for tasks yet (will be done below)... Sooo will think about it...

		}
		// logger.info("Sync job");
		// this.jobs.set(this.jobs.indexOf(job), job);
		// logger.info("Synced job");
		// jobBCMessageUpdateCondition.jobId(job.id()).types(finishedAllTasksMessagesToRemove);
		// synchronized (this.bcMessages) {
		// BlockingQueue<IBCMessage> tmp = new PriorityBlockingQueue<IBCMessage>();
		// for (IBCMessage t : this.bcMessages) {
		// if (jobBCMessageUpdateCondition.metBy(t)) {
		// tmp.add(t);
		// }
		// }
		// this.bcMessages = tmp;
		// }

		List<Task> tasks = job.subsequentProcedure().tasks();
		List<FutureGet> futureGets = SyncedCollectionProvider.syncedArrayList();
		List<FuturePut> futurePuts = SyncedCollectionProvider.syncedArrayList();
		for (Task task : tasks) {
			logger.info("task: " + task);
			List<Tuple<String, Integer>> finalExecutorTaskDomainParts = task.finalExecutorTaskDomainParts();
			logger.info("finalExecutorTaskDomainParts: " + finalExecutorTaskDomainParts);
			for (Tuple<String, Integer> finalDataLocationDomain : finalExecutorTaskDomainParts) {
				logger.info("finalDataLocationDomain: " + finalDataLocationDomain);
				Tuple<String, Tuple<String, Integer>> executorTaskDomain = task.executorTaskDomain(finalDataLocationDomain);
				String combination = task.concatenationString(finalDataLocationDomain);
				logger.info("get task keys for task executor domain: " + combination);
				futureGets.add(this.jobExecutor.dhtConnectionProvider().getAll(DomainProvider.TASK_KEYS, combination)
						.addListener(new BaseFutureAdapter<FutureGet>() {

							@Override
							public void operationComplete(FutureGet future) throws Exception {
								if (future.isSuccess()) {
									logger.info("Success on retrieving task keys for task executor domain: " + combination);
									try {
										// logger.info("future.dataMap() != null: " + (future.dataMap() != null));
										// if (future.dataMap() != null) {
										Set<Number640> keySet = future.dataMap().keySet();
										logger.info("KeySet: " + keySet);
										for (Number640 n : keySet) {
											String key = (String) future.dataMap().get(n).object();
											logger.info("Key: " + key);
											futurePuts.add(jobExecutor.dhtConnectionProvider().add(key, executorTaskDomain,
													job.subsequentProcedure().jobProcedureDomainString(), false));
											futurePuts.add(jobExecutor.dhtConnectionProvider().add(DomainProvider.PROCEDURE_KEYS, key,
													job.subsequentProcedure().jobProcedureDomainString(), false));
										}
										// }
									} catch (IOException e) {
										logger.warn("IOException on getting the data", e);
									}
								} else {
									logger.info("No success retrieving task keys form task executor domain: " + combination);
								}
							}
						}));

			}

		}
		logger.info("futureGets: " + futureGets);
		if (futureGets.size() > 0) {
			Futures.whenAllSuccess(futureGets).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

				@Override
				public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
					logger.info("futurePuts: " + futurePuts);
					if (future.isSuccess()) {
						Futures.whenAllSuccess(futurePuts).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

							@Override
							public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {

								if (future.isSuccess()) {
									// well... check if there is actually any procedure left... Else job is finished...
									job.incrementCurrentProcedureIndex();
									ProcedureInformation subsequentProcedure = job.subsequentProcedure();
									job.currentProcedure().tasks().clear();
									if (subsequentProcedure.procedure().getClass().getSimpleName().equals(EndReached.class.getSimpleName())) {
										// Finished job :)
										logger.info("Finished job");
										FinishedJobBCMessage message = jobExecutor.dhtConnectionProvider().broadcastFinishedJob(job);
										bcMessages.add(message);
										// jobs.remove(job);
									} else {
										// Next procedure!!
										logger.info("Execute next procedure");
										DistributedJobBCMessage message = jobExecutor.dhtConnectionProvider().broadcastNewJob(job);
										bcMessages.add(message);
										logger.info("job.currentProcedure()  : " + job.currentProcedure());
									}
								} else {
									// TODO Well... something has to be done instead... am I right?
									logger.info("No success");
								}
							}

						});
					} else {
						logger.info("No success");
					}
				}

			});

		} else {
			logger.warn("No FuturePuts created. Check why?");
		}
		System.err.println("END");
	}

	@Override
	public void handleFinishedJob(Job job) {
		logger.info("received job finished message: " + job.id());
		jobs.remove(jobs.indexOf(job));
	}

	@Override
	public void handleFailedJob(Job job) {
		// TODO Auto-generated method stub

	}

}
