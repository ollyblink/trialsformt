package mapreduce.manager;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;

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
import mapreduce.utils.Value;
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
		Procedure procedure = job.currentProcedure();
		procedure.isActive(true);
		PriorityBlockingQueue<IBCMessage> bcMessages = messageConsumer.queueFor(job);

		String outputPExecutable = procedure.executable().getClass().getSimpleName();
		int outputPIndex = procedure.procedureIndex();
		String outputPExecutor = this.id();
		JobProcedureDomain outputJPD = new JobProcedureDomain(job.id(), outputPExecutor, outputPExecutable, outputPIndex);

		// Tries to find task keys on the network
		getTaskKeys(job, bcMessages, procedure, outputJPD);

		logger.info("Scheduling tasks for execution in case task was received from broadcast.");
		// In parallel to finding task keys on the network, execution starts in case a task was received from broadcast
		execute(job, bcMessages, procedure, outputJPD);

	}

	private void getTaskKeys(Job job, PriorityBlockingQueue<IBCMessage> bcMessages, Procedure procedure, JobProcedureDomain outputJPD) {
		logger.info("Retrieve task keys for input procedure domain: " + procedure.inputDomain() + ".");
		dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, procedure.inputDomain().toString())
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							procedure.tasksSize(future.dataMap().size());
							for (Number640 keyHash : future.dataMap().keySet()) {
								String key = (String) future.dataMap().get(keyHash).object();
								logger.info("Found key: " + key);
								Task2 task = Task2.create(key);
								if (!procedure.tasks().contains(task)) {// Don't need to add it more, got it e.g. from a BC
									logger.info("Added key " + key + ", scheduling task for execution.");
									procedure.tasks().add(task);
									execute(job, bcMessages, procedure, outputJPD);
								}
							}
						} else {
							logger.info("Failed to retrieve task keys for input job procedure domain: " + procedure.inputDomain());
						}
					}

				});
	}

	private void execute(Job job, PriorityBlockingQueue<IBCMessage> bcMessages, Procedure procedure, JobProcedureDomain outputJPD) {
		if (procedure.tasks().size() != 0) {
			Task2 nextTask = taskExecutionScheduler.schedule(procedure.tasks());
			if (nextTask == null) {
				if (procedure.tasksSize() > procedure.tasks().size()) {
					logger.info("Do nothing, as there may come more tasks from dht");
				} else if (procedure.tasksSize() == procedure.tasks().size()) {
					logger.info("Finishing procedure");
					// TODO: CHECK OB ALLE TASKS IN PROCEDUREDOMAIN (e.g. task.inProcedureDomain())
					for (Task2 task : procedure.tasks()) {
						if (task.isFinished()) {
							if (!task.isInProcedureDomain()) {
								tryToAddTaskDataToProcedureDomain(task, outputJPD);
							}
						} else {
							logger.info("Should not get here: tasks is not finished!");
						}
					}
				} else if (procedure.tasksSize() < procedure.tasks().size()) {
					logger.warn("Can only happen when a task is received from a broadcast first --> Set in broadcast (should have happend)");
				}
			} else {
				logger.info("Executing task: " + nextTask.key());
				executeTask(bcMessages, nextTask, procedure, outputJPD);

			}
		} else {
			logger.info("No tasks yet... Keep waiting until we receive something.");
		}
	}

	public void executeTask(PriorityBlockingQueue<IBCMessage> bcMessages, Task2 task, Procedure procedure, JobProcedureDomain outputJPD) {
		if (canExecute()) {
			this.executionCounter++;
			task.isActive(true);
			logger.info("Task to execute: " + task);
			// Now we actually wanna retrieve the data from the specified locations...
			dhtConnectionProvider.getAll(task.key(), procedure.inputDomain().toString()).addListener(new BaseFutureAdapter<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {
					if (future.isSuccess()) {
						List<Object> values = syncedArrayList();
						Set<Number640> valueSet = future.dataMap().keySet();
						for (Number640 valueHash : valueSet) {
							Object taskValue = ((Value) future.dataMap().get(valueHash).object()).value();
							values.add(taskValue);
						}
						// Start execution on successful retrieval
						// Everything here with subsequent procedure!!!!
						ExecutorTaskDomain outputETD = new ExecutorTaskDomain(task.key(), id, task.nextStatusIndexFor(id), outputJPD);
						IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).task(task)
								.dhtConnectionProvider(dhtConnectionProvider);

						logger.info("Executing task: " + task.key() + " with values " + values);
						procedure.executable().process(task.key(), values, context);

						Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
							@Override
							public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
								if (future.isSuccess()) {
									outputETD.resultHash(context.resultHash());
									CompletedBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(outputETD, procedure.inputDomain(),
											procedure.tasksSize());
									bcMessages.add(msg); // Adds it to itself, does not receive broadcasts... Makes sure this result is ignored in
															// case another was received already
									dhtConnectionProvider.broadcastCompletion(msg);
									task.isActive(false);
									--executionCounter;
								} else {
									logger.warn("No success on task execution. Reason: " + future.failedReason());
								}
							}

						});
					} else {
						logger.info("Could not retrieve data for task " + task.key() + " in job procedure domain: "
								+ procedure.inputDomain().toString() + ". Failed reason: " + future.failedReason());
					}

				}

			});
		} else {
			logger.info("Cannot execute due to too many currently running executions: " + this.executionCounter
					+ " executions running at a max number of " + maxNrOfExecutions + " executions.");
		}

	}

	public FutureDone<List<FutureGet>> tryToAddTaskDataToProcedureDomain(Task2 task, JobProcedureDomain outputJobProcedureDomain) {
		task.isActive(true);
		List<FutureGet> futureGetKeys = syncedArrayList();

		ExecutorTaskDomain resultOutputDomain = (ExecutorTaskDomain) task.resultOutputDomain();

		futureGetKeys.add(dhtConnectionProvider.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, resultOutputDomain.toString())
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							Set<Number640> keySet = future.dataMap().keySet();

							for (Number640 n : keySet) {
								String taskOutputKey = (String) future.dataMap().get(n).object();
								futureGetKeys.add(dhtConnectionProvider.getAll(taskOutputKey, resultOutputDomain.toString())
										.addListener(new BaseFutureAdapter<FutureGet>() {

									@Override
									public void operationComplete(FutureGet future) throws Exception {
										if (future.isSuccess()) {
											dhtConnectionProvider
													.addAll(taskOutputKey, future.dataMap().values(), outputJobProcedureDomain.toString())
													.addListener(new BaseFutureAdapter<FuturePut>() {

												@Override
												public void operationComplete(FuturePut future) throws Exception {
													if (future.isSuccess()) {
														dhtConnectionProvider
																.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, taskOutputKey,
																		outputJobProcedureDomain.toString(), false)
																.addListener(new BaseFutureAdapter<FuturePut>() {

															@Override
															public void operationComplete(FuturePut future) throws Exception {
																if (future.isSuccess()) {
																	logger.info("Successfully added task output key and values for task output key "
																			+ taskOutputKey + " to output procedure domain "
																			+ outputJobProcedureDomain.toString());
																} else {
																	logger.info("Failed to add task output key and values for task output key "
																			+ taskOutputKey + " to output procedure domain "
																			+ outputJobProcedureDomain.toString() + ", failed reason: "
																			+ future.failedReason());
																}
															}

														});
													} else {
														logger.info("Failed to add values for task output key " + taskOutputKey
																+ " to output procedure domain " + outputJobProcedureDomain.toString()
																+ ", failed reason: " + future.failedReason());
													}
												}

											});

										} else {
											logger.info("Failed to get task output key and values for task output key (" + taskOutputKey
													+ " from task executor domain " + resultOutputDomain.toString() + ", failed reason: "
													+ future.failedReason());

										}
									}
								}));

							}
						} else {
							logger.info("Failed to get task keys for task " + task.key() + " from task executor domain "
									+ resultOutputDomain.toString() + ", failed reason: " + future.failedReason());
						}
					}
				}));

		return Futures.whenAllSuccess(futureGetKeys).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

			@Override
			public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
				if (future.isSuccess()) {
					task.isInProcedureDomain(true);
					logger.info("Success on putting task output keys for task " + task.key() + " into output procedure domain "
							+ outputJobProcedureDomain.toString());
				} else {
					logger.info("Failed to put task output keys for task " + task.key() + " into output procedure domain "
							+ outputJobProcedureDomain.toString() + ", failed reason: " + future.failedReason());
				}
				task.isActive(false);
				--executionCounter;
			}

		});

	}

	public void abortExecution() {
		// TODO Auto-generated method stub

	}

	public boolean canExecute() {
		return executionCounter < this.maxNrOfExecutions;
	}

	// End Execution

}
