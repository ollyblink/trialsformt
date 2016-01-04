package mapreduce.manager;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task2;
import mapreduce.execution.task.scheduling.ITaskScheduler;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.CompletedBCMessage;
import mapreduce.manager.broadcasting.broadcastmessages.IBCMessage;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.IDCreator;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	private IDHTConnectionProvider dhtConnectionProvider;
	private MRJobExecutionManagerMessageConsumer messageConsumer;

	private String id;

	private volatile int executionCounter;

	private ITaskScheduler taskExecutionScheduler;

	private int maxNrOfExecutions;

	private MRJobExecutionManager() {

	}

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobExecutionManagerMessageConsumer.create(this);
		this.dhtConnectionProvider = dhtConnectionProvider.owner(this.id).jobQueues(messageConsumer.jobs());
	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider).taskExecutionScheduler(MinAssignedWorkersTaskExecutionScheduler.newInstance())
				.maxNrOfExecutions(Runtime.getRuntime().availableProcessors());
	}

	public MRJobExecutionManager taskExecutionScheduler(ITaskScheduler taskExecutionScheduler) {
		this.taskExecutionScheduler = taskExecutionScheduler;
		return this;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public MRJobExecutionManager maxNrOfExecutions(int maxNrOfExecutions) {
		this.maxNrOfExecutions = maxNrOfExecutions;
		return this;
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
															task.addInputDomain(inputDomain);
														}
														if (!tasks.contains(task)) {
															tasks.add(task);
															execute(job, currentProcedure, bcMessages, maxNrOfSubmissions, tasks,
																	outputJobProcedureDomain);

														}
													} catch (IOException e) {
														logger.info("failed");
													}
												} else {
													logger.info("failed");
												}
											}

										});
									}
								}
							} catch (IOException e) {
								logger.info("failed");
							}
						} else {
							logger.info("failed");
						}
					}

				});

		execute(job, currentProcedure, bcMessages, maxNrOfSubmissions, tasks, outputJobProcedureDomain);

	}

	private void execute(Job job, Procedure currentProcedure, PriorityBlockingQueue<IBCMessage> bcMessages, int maxNrOfSubmissions, List<Task2> tasks,
			JobProcedureDomain outputJobProcedureDomain) {
		if (currentProcedure.tasks().size() != 0) {

			Task2 nextTask = taskExecutionScheduler.schedule(tasks);
			if (nextTask == null) {
				if (currentProcedure.tasksSize() > currentProcedure.tasks().size()) {
					logger.info("Do nothing, as there may come more tasks from dht");
				} else if (currentProcedure.tasksSize() == currentProcedure.tasks().size()) {
					logger.info("Finishing procedure");
					// TODO: CHECK OB ALLE TASKS IN PROCEDUREDOMAIN (e.g. task.inProcedureDomain())
					List<FutureDone<List<FutureGet>>> allKeysPut = syncedArrayList();
					for (Task2 task : currentProcedure.tasks()) {
						if (task.isFinished()) {
							if (!task.isInProcedureDomain()) {
								allKeysPut.add(tryToAddTaskDataToProcedureDomain(task, maxNrOfSubmissions, outputJobProcedureDomain));
							}
						} else {
							logger.info("Should not get here: tasks is not finished!");
						}
					}
					Futures.whenAllSuccess(allKeysPut).addListener();
				} else if (currentProcedure.tasksSize() < currentProcedure.tasks().size()) {
					logger.warn("Can only happen when a task is received from a broadcast first --> Set in broadcast (should have happend)");
				}
			} else {
				logger.info("Executing task: " + nextTask.key());
				executeTask(nextTask, currentProcedure, outputJobProcedureDomain, maxNrOfSubmissions, job.currentProcedure().inputDomain(),
						bcMessages);

			}
		}
	}

	public FutureDone<List<FutureGet>> tryToAddTaskDataToProcedureDomain(Task2 task, int maxNrOfSubmissions,
			JobProcedureDomain outputJobProcedureDomain) {
		task.isActive(true);
		List<FutureGet> futureGetKeys = syncedArrayList();

		logger.info("task: " + task);
		ExecutorTaskDomain resultOutputDomain = (ExecutorTaskDomain) task.resultOutputDomain();

		logger.info("get task keys for task executor domain: " + resultOutputDomain.toString());
		futureGetKeys.add(dhtConnectionProvider.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, resultOutputDomain.toString())
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							List<FutureGet> futureGetValues = syncedArrayList();
							logger.info("Success on retrieving task keys for task executor domain: " + resultOutputDomain.toString());
							Set<Number640> keySet = future.dataMap().keySet();
							logger.info("KeySet: " + keySet);

							for (Number640 n : keySet) {
								String taskKey = (String) future.dataMap().get(n).object();
								futureGetValues.add(dhtConnectionProvider.getAll(taskKey, resultOutputDomain.toString())
										.addListener(new BaseFutureAdapter<FutureGet>() {

									@Override
									public void operationComplete(FutureGet future) throws Exception {
										if (future.isSuccess()) {
											List<FuturePut> taskKeyPut = syncedArrayList();
											List<FuturePut> taskKeyValuesPut = syncedArrayList();
											taskKeyValuesPut.add(dhtConnectionProvider
													.addAll(taskKey, future.dataMap().values(), outputJobProcedureDomain.toString())
													.addListener(new BaseFutureAdapter<FuturePut>() {

												@Override
												public void operationComplete(FuturePut future) throws Exception {
													if (future.isSuccess()) {
														taskKeyPut.add(dhtConnectionProvider
																.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, taskKey,
																		outputJobProcedureDomain.toString(), false)
																.addListener(new BaseFutureAdapter<FuturePut>() {

															@Override
															public void operationComplete(FuturePut future) throws Exception {
																if (future.isSuccess()) {

																} else {

																}
															}

														}));
													}
												}

											}));
											Futures.whenAllSuccess(taskKeyValuesPut).addListener(new BaseFutureAdapter<FutureDone<FuturePut>>() {

												@Override
												public void operationComplete(FutureDone<FuturePut> future) throws Exception {
													if (future.isSuccess()) {
														Futures.whenAllSuccess(taskKeyPut)
																.addListener(new BaseFutureAdapter<FutureDone<FuturePut>>() {

															@Override
															public void operationComplete(FutureDone<FuturePut> future) throws Exception {
																if (future.isSuccess()) {

																} else {

																}
															}
														});
													} else {

													}
												}
											});
										} else {
											// TODO
										}
									}
								}));

							}
							Futures.whenAllSuccess(futureGetValues).addListener(new BaseFutureAdapter<FutureDone<FuturePut>>() {

								@Override
								public void operationComplete(FutureDone<FuturePut> future) throws Exception {
									if (future.isSuccess()) {

									} else {

									}

								}
							});
							// }

						} else {
							logger.info("No success retrieving task keys form task executor domain: " + outputJobProcedureDomain.toString());
						}
					}
				}));

		return Futures.whenAllSuccess(futureGetKeys).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

			@Override
			public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
				if (future.isSuccess()) {
					task.isInProcedureDomain(true);
				} else {

				}
				task.isActive(false);
				--executionCounter;
			}

		});

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
					if (future.isSuccess()) {
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
									CompletedBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(outputExecutorTaskDomain,
											initialInputDomain, procedure.tasksSize());
									bcMessages.add(msg); // Adds it to itself, does not receive broadcasts...
									dhtConnectionProvider.broadcastCompletion(msg);
									task.isActive(false);
									--executionCounter;
								} else {
									logger.warn("No success on task execution. Reason: " + future.failedReason());
								}
							}

						});
					} else {
						logger.warn("No success on task execution. Reason: " + future.failedReason());
					}
				}
			});
		}

	}

	public void abortExecution() {
		// TODO Auto-generated method stub

	}

	public boolean canExecute() {
		return executionCounter < this.maxNrOfExecutions;
	}

	// End Execution

}
