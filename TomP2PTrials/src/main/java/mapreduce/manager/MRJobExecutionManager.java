package mapreduce.manager;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.IDomain;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.ExecutorTaskDomain;
import mapreduce.execution.procedures.JobProcedureDomain;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task2;
import mapreduce.execution.task.scheduling.ITaskScheduler;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.CompletedBCMessage;
import mapreduce.manager.broadcasting.broadcastmessages.IBCMessage;
import mapreduce.manager.broadcasting.broadcastmessages.jobmessages.JobDistributedBCMessage;
import mapreduce.manager.broadcasting.broadcastmessages.jobmessages.JobFinishedBCMessage;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
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

	// private Job currentlyExecutedJob;
	private volatile int executionCounter;

	// private List<Future<?>> activeThreads = SyncedCollectionProvider.syncedArrayList();

	private ITaskScheduler taskExecutionScheduler;

	private MRJobExecutionManager() {

	}

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobExecutionManagerMessageConsumer.create(this);
		this.dhtConnectionProvider = dhtConnectionProvider.owner(this.id).addMessageQueueToBroadcastHandler(messageConsumer.jobs());
	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider).taskExecutionScheduler(MinAssignedWorkersTaskExecutionScheduler.newInstance());
	}

	public MRJobExecutionManager taskExecutionScheduler(ITaskScheduler taskExecutionScheduler) {
		this.taskExecutionScheduler = taskExecutionScheduler;
		return this;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

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

	public void executeJob(Job job) {
		Procedure currentProcedure = job.currentProcedure().isActive(true);
		PriorityBlockingQueue<IBCMessage> bcMessages = messageConsumer.queueFor(job);
		this.taskExecutionScheduler.procedureInformation(currentProcedure);
		int maxNrOfSubmissions = job.maxNrOfDHTActions();
		// Get the data for the job's current procedure
		// this.server = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		String inputJobProcedureDomainString = DomainProvider.INSTANCE.jobProcedureDomain(job.currentProcedure().inputDomain());
		// logger.info("Got job: " + currentlyExecutedJob.id() + ", retrieving data for , " + dataLocationJobProcedureDomainString);

		// Get all procedure keys!! Create all the tasks for each key!!!
		List<Task2> tasks = currentProcedure.tasks();

		JobProcedureDomain outputJobProcedureDomain = new JobProcedureDomain(job.id(), id(), currentProcedure.executable().getClass().getSimpleName(),
				currentProcedure.procedureIndex());
		logger.info("Retrieve data for domain: " + inputJobProcedureDomainString);
		dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, inputJobProcedureDomainString)
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						logger.info("Job Proc domain: " + inputJobProcedureDomainString);
						if (future.isSuccess()) {
							currentProcedure.tasksSize(future.dataMap().size());
							try {
								for (Number640 n : future.dataMap().keySet()) {
									String key = (String) future.dataMap().get(n).object();
									logger.info("Key: " + key);
									Task2 task = Task2.create(key);

									if (tasks.contains(task)) {// Don't need to add it more, got it e.g. from a BC
										logger.info("tasks.contains(" + task + "): " + tasks.contains(task));
										return;
									} else {
										logger.info("Get <" + task.key() + "," + inputJobProcedureDomainString + ">");
										dhtConnectionProvider.getAll(task.key(), inputJobProcedureDomainString)
												.addListener(new BaseFutureAdapter<FutureGet>() {

											@Override
											public void operationComplete(FutureGet future) throws Exception {
												if (future.isSuccess()) {
													try {
														for (Number640 n : future.dataMap().keySet()) {
															ExecutorTaskDomain inputDomain = (ExecutorTaskDomain) future.dataMap().get(n).object();

															logger.info("inputDomain: " + inputDomain);
															task.addInputDomain(inputDomain);
														}
														if (!tasks.contains(task)) {
															logger.info("!tasks.contains(task): " + task);
															tasks.add(task);
															executeTask(taskExecutionScheduler.schedule(tasks), currentProcedure,
																	outputJobProcedureDomain, maxNrOfSubmissions,
																	job.currentProcedure().inputDomain(), bcMessages);

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
		executeTask(taskExecutionScheduler.schedule(tasks), currentProcedure, outputJobProcedureDomain, maxNrOfSubmissions,
				job.currentProcedure().inputDomain(), bcMessages);

	}

	public void executeTask(Task2 task, Procedure procedure, JobProcedureDomain outputJobProcedureDomain, int maxNrOfSubmissions,
			JobProcedureDomain initialInputDomain, PriorityBlockingQueue<IBCMessage> bcMessages) {
		if (canExecute()) {
			this.executionCounter++;
			task.isActive(true);
			logger.info("Task to execute: " + task);

			List<Object> valuesCollector = syncedArrayList();
			List<FutureGet> futureGetData = syncedArrayList();

			// TODO build in that the data retrieval may take a certain number of repetitions when failed before being broadcasted as failed
			for (ExecutorTaskDomain inputDomain : task.inputDomains()) {
				// Now we actually wanna retrieve the data from the specified locations...
				futureGetData.add(dhtConnectionProvider
						.getAll(inputDomain.taskId(), DomainProvider.INSTANCE.concatenation(inputDomain.jobProcedureDomain(), inputDomain))
						.addListener(new GetTaskValuesListener(inputDomain, valuesCollector, 1, maxNrOfSubmissions, dhtConnectionProvider)));
			}
			// Start execution on successful retrieval
			// Everything here with subsequent procedure!!!!

			Futures.whenAllSuccess(futureGetData).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
				@Override
				public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {

					ExecutorTaskDomain outputExecutorTaskDomain = new ExecutorTaskDomain(task.key(), id, task.nextStatusIndexFor(id),
							outputJobProcedureDomain);
					IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputExecutorTaskDomain).task(task)
							.dhtConnectionProvider(dhtConnectionProvider);

					logger.info("Executing task: " + task.key() + " with values " + valuesCollector);
					procedure.executable().process(task.key(), valuesCollector, context);

					Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
						@Override
						public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
							if (future.isSuccess()) {
								outputExecutorTaskDomain.resultHash(context.resultHash());
								task.addOutputDomain(outputExecutorTaskDomain);

								Task2 nextTask = taskExecutionScheduler.schedule(procedure.tasks()); // Calls task.isFinished();
								if (nextTask == null) {// means that all tasks are finished

									List<FutureGet> futureGets = syncedArrayList();
									List<FuturePut> futurePuts = syncedArrayList();

									List<Task2> tasks = procedure.tasks();
									for (Task2 task : tasks) {
										logger.info("task: " + task);
										ExecutorTaskDomain resultOutputDomain = (ExecutorTaskDomain) task.resultOutputDomain();

										logger.info("get task keys for task executor domain: " + resultOutputDomain.toString());
										futureGets.add(
												dhtConnectionProvider.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, resultOutputDomain.toString())
														.addListener(new BaseFutureAdapter<FutureGet>() {

											@Override
											public void operationComplete(FutureGet future) throws Exception {
												if (future.isSuccess()) {
													logger.info("Success on retrieving task keys for task executor domain: "
															+ resultOutputDomain.toString());
													try {
														// logger.info("future.dataMap() != null: " + (future.dataMap() != null));
														// if (future.dataMap() != null) {
														Set<Number640> keySet = future.dataMap().keySet();
														logger.info("KeySet: " + keySet);
														for (Number640 n : keySet) {
															String key = (String) future.dataMap().get(n).object();
															logger.info("Key: " + key);
															futurePuts.add(dhtConnectionProvider.add(key, resultOutputDomain,
																	outputJobProcedureDomain.toString(), false));
															futurePuts.add(dhtConnectionProvider.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, key,
																	outputJobProcedureDomain.toString(), false));
														}
														// }
													} catch (IOException e) {
														logger.warn("IOException on getting the data", e);
													}
												} else {
													logger.info("No success retrieving task keys form task executor domain: "
															+ outputJobProcedureDomain.toString());
												}
											}
										}));

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
															task.isActive(false);
															procedure.isActive(false);
															--executionCounter;
															if (future.isSuccess()) {
																procedure.addOutputDomain(outputJobProcedureDomain);
																if (procedure.isFinished()) {
																	CompletedBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(
																			outputExecutorTaskDomain, initialInputDomain);
																	bcMessages.add(msg); // Adds it to itself, does not receive broadcasts...
																	dhtConnectionProvider.broadcastTaskCompleted(msg);

																} else {
																	procedure.reset();
																	executeJob(messageConsumer.nextJob());
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
								} else {
									CompletedBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(outputExecutorTaskDomain,
											initialInputDomain);
									bcMessages.add(msg); // Adds it to itself, does not receive broadcasts...
									dhtConnectionProvider.broadcastTaskCompleted(msg);
									procedure.isActive(false);
									task.isActive(false);
									--executionCounter;
								}
							} else {
								logger.warn("No success on task execution. Reason: " + future.failedReason());
							}
						}

					});
				}
			});
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
