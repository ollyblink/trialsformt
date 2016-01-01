package mapreduce.manager;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.computation.context.DHTStorageContext;
import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.Tasks;
import mapreduce.execution.task.scheduling.ITaskScheduler;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasting.broadcastmessages.jobmessages.ProcedureFinishedBCMessage;
import mapreduce.manager.broadcasting.broadcastmessages.jobmessages.TaskUpdateBCMessage;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	// private static final ITaskExecutor DEFAULT_TASK_EXCECUTOR = ParallelTaskExecutor.newInstance();
	private static final int MAX_EXECUTIONS = 1;
	private IDHTConnectionProvider dhtConnectionProvider;
	// private IContext context;
	// private ITaskExecutor taskExecutor;
	// private List<Job> jobs;
	private MRJobExecutionManagerMessageConsumer messageConsumer;

	// private Future<?> createTaskThread;

	// private ThreadPoolExecutor server;

	private String id;

	private Job currentlyExecutedJob;
	private volatile int executionCounter;

	// private List<Future<?>> activeThreads = SyncedCollectionProvider.syncedArrayList();

	private ITaskScheduler taskExecutionScheduler;
 

	private MRJobExecutionManager() {

	}

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobExecutionManagerMessageConsumer.newInstance().jobExecutor(this);
		this.dhtConnectionProvider = dhtConnectionProvider.owner(this.id).addMessageQueueToBroadcastHandler(messageConsumer.queue());
	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider).taskExecutionScheduler(MinAssignedWorkersTaskExecutionScheduler.newInstance());
	}

	public MRJobExecutionManager taskExecutionScheduler(ITaskScheduler taskExecutionScheduler) {
		this.taskExecutionScheduler = taskExecutionScheduler;
		return this;
	}

	public Job currentlyExecutedJob() {
		return this.currentlyExecutedJob;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	// public MRJobExecutionManager context(IContext context) {
	// this.context = context;
	// this.context.dhtConnectionProvider(this.dhtConnectionProvider);
	//
	// return this;
	// }

	// END GETTER/SETTER

	// Maintenance

	public void shutdown() {
		this.messageConsumer.canTake(false);
		dhtConnectionProvider.shutdown();
	}

	public String id() {
		return this.id;
	}

	public void start() {
		// this.dhtConnectionProvider.connect();
		messageConsumer.canTake(true);
		Thread messageConsumerThread = new Thread(messageConsumer);
		messageConsumerThread.start();
	}

	// End Maintenance
	// Execution

	public void execute(Job job) { 
		this.currentlyExecutedJob = job.isActive(true);
		// Get the data for the job's current procedure
		// this.server = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		String dataLocationJobProcedureDomainString = job.previousProcedure().jobProcedureDomainString();
		logger.info("Got job: " + currentlyExecutedJob.id() + ", retrieving data for , " + dataLocationJobProcedureDomainString);

		Tuple<String, Tuple<String, Integer>> subsequentJobProcedureDomain = job.currentProcedure().jobProcedureDomain();

		// Get all procedure keys!! Create all the tasks for each key!!!
		List<Task> tasks = job.currentProcedure().tasks();
		logger.info("Retrieve data for domain: " + dataLocationJobProcedureDomainString);
		dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_KEYS, dataLocationJobProcedureDomainString)
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						logger.info("Job Proc domain: " + dataLocationJobProcedureDomainString);
						if (future.isSuccess()) {
							try {
								for (Number640 n : future.dataMap().keySet()) {
									String key = (String) future.dataMap().get(n).object();
									logger.info("Key: " + key);
									Task task = Task.create(key, subsequentJobProcedureDomain);

									if (tasks.contains(task)) {// Don't need to add it more, got it e.g. from a BC
										logger.info("tasks.contains(" + task + "): " + tasks.contains(task));
										return;
									} else {
										logger.info("Get <" + task.id() + "," + dataLocationJobProcedureDomainString + ">");
										dhtConnectionProvider.getAll(task.id(), dataLocationJobProcedureDomainString)
												.addListener(new BaseFutureAdapter<FutureGet>() {

											@Override
											public void operationComplete(FutureGet future) throws Exception {
												if (future.isSuccess()) {
													try {
														for (Number640 n : future.dataMap().keySet()) {
															Tuple<String, Tuple<String, Integer>> initialDataLocation = (Tuple<String, Tuple<String, Integer>>) future
																	.dataMap().get(n).object();

															logger.info("taskExecutor: " + initialDataLocation);
															task.addInitialExecutorTaskDomain(initialDataLocation);
														}
														if (!tasks.contains(task)) {
															logger.info("!tasks.contains(task)");
															tasks.add(task);
															if (canExecute()) {
																executeTask(taskExecutionScheduler.schedule(tasks));
															}
														}
													} catch (IOException e) {
														logger.info("failed");
														// dhtConnectionProvider.broadcastFailedTask(taskToDistribute);
													}
												} else {
													// dhtConnectionProvider.broadcastFailedJob(jobs.get(0));
													logger.info("failed");
												}
											}

										});
									}
								}
							} catch (IOException e) {
								// dhtConnectionProvider.broadcastFailedTask(taskToDistribute);
								logger.info("failed");
							}
						} else {
							// dhtConnectionProvider.broadcastFailedJob(jobs.get(0));
							logger.info("failed");
						}
					}

				});

	}

	public void executeTask(Task task) {
		if (!isExecutionAborted() && task != null && !task.isActive()) {
			this.executionCounter++;
			logger.info("Task to execute: " + task);

			List<Tuple<String, Tuple<String, Integer>>> executorTaskDomains = task.initialExecutorTaskDomain();
			List<Object> valuesCollector = syncedArrayList();
			List<FutureGet> futureGetData = syncedArrayList();
			Tuple<String, Integer> taskExecutor = null;
			synchronized (task.executingPeers()) {
				task.executingPeers().put(id, BCMessageStatus.EXECUTING_TASK);
				taskExecutor = Tuple.create(id, task.executingPeers().get(id).size() - 1);
			}

			messageConsumer.queue().add(dhtConnectionProvider.broadcastExecutingTask(task, taskExecutor));

			// TODO build in that the data retrieval may take a certain number of repetitions when failed before being broadcasted as failed
			for (Tuple<String, Tuple<String, Integer>> eTD : executorTaskDomains) {
				// Now we actually wanna retrieve the data from the specified locations...
				Task oldTask = Task.create(eTD.first(), currentlyExecutedJob.previousProcedure().jobProcedureDomain());
				String executorTaskDomain = oldTask.concatenationString(eTD.second());
				logger.info("Data for executorTaskDomain " + executorTaskDomain);
				futureGetData.add(collectValuesForTask(task, valuesCollector, executorTaskDomain));

			}
			// Start execution on successful retrieval
			// Everything here with subsequent procedure!!!!
			Futures.whenAllSuccess(futureGetData).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
				@Override
				public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {

					IContext context = DHTStorageContext.create().taskExecutor(taskExecutor).task(task).dhtConnectionProvider(dhtConnectionProvider)
							.subsequentProcedure(currentlyExecutedJob.currentProcedure());
					// taskExecutor.execute(job.currentProcedure().procedure(), task.id(), valuesCollector, context);

					ProcedureInformation currentPI = currentlyExecutedJob.currentProcedure();
					task.isActive(true);
					logger.info("Executing task: " + task.id() + " with values " + valuesCollector);
					currentPI.procedure().process(task.id(), valuesCollector, context);

					Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
						@Override
						public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
							if (future.isSuccess()) {
								task.isActive(false);
								executionCounter--;
								Task nextTask = taskExecutionScheduler.procedureInformation(currentPI).schedule(currentPI.tasks());
								if (currentPI.isFinished()) {
									// May have been aborted from outside and thus, hasn't finished yet (in case abortExecution(job) was called
									currentlyExecutedJob.isActive(false);
									messageConsumer.queue().add(dhtConnectionProvider.broadcastFinishedAllTasksOfProcedure(currentlyExecutedJob));
									logger.info("Broadcast finished Procedure");
								} else {
									messageConsumer.queue()
											.add(dhtConnectionProvider.broadcastFinishedTask(task, taskExecutor, context.resultHash()));
									logger.info("executing next task");
									logger.info("tasks: " + currentPI.tasks());
									if (canExecute()) {
										executeTask(nextTask);
									}
								}
							} else {
								// TASK FAILED
								// messageConsumer.queue()
								// .add(dhtConnectionProvider.broadcastFinishedTask(task, taskExecutor, context.resultHash()));
								// logger.info("executing next task");
								// logger.info("tasks: " + subsequentProcedureInformation.tasks());
								// if (canExecute()) {
								// executeTask(nextTask);
								// }
							}
						}

					});
				}
			});
		}

	}
	//
	// private FutureGet retrieveDataForTask(Task task, String taskExecutorDomain, List<Object> valuesCollector) {
	// return
	// }

	private FutureGet collectValuesForTask(Task task, List<Object> valuesCollector, String taskExecutorDomainConcatenation) {
		return dhtConnectionProvider.getAll(task.id(), taskExecutorDomainConcatenation).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					try {
						if (future.dataMap() != null) {
							for (Number640 n : future.dataMap().keySet()) {
								valuesCollector.add(((Value) future.dataMap().get(n).object()).value());
							}
						}
					} catch (IOException e) {
						logger.warn("IOException on getting the data", e);
					}
				} else {
					logger.info("No success on retrieving data for key : " + task.id());
				}
			}

		});
	}

	public boolean isExecutionAborted() {
		if (this.currentlyExecutedJob != null) {
			return this.currentlyExecutedJob.isActive();
		} else {
			return true;
		}
	}

	public void isExecutionAborted(boolean isExecutionAborted) {
		if (this.currentlyExecutedJob != null) {
			this.currentlyExecutedJob.isActive(false);
		}
	}

	public boolean canExecute() {
		return executionCounter < MAX_EXECUTIONS;
	}

	public ITaskScheduler taskExecutionScheduler() {
		return this.taskExecutionScheduler;
	}

	// End Execution

}
