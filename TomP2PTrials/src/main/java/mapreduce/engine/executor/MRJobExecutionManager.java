package mapreduce.engine.executor;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.CompletedBCMessage;
import mapreduce.engine.broadcasting.IBCMessage;
import mapreduce.engine.messageConsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.scheduling.ITaskScheduler;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
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
import net.tomp2p.storage.Data;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	private IDHTConnectionProvider dhtCon;
	private MRJobExecutionManagerMessageConsumer messageConsumer;

	private String id;

	private volatile int executionCounter;

	private ITaskScheduler taskExecutionScheduler;

	private int maxNrOfExecutions = 1;

	private MRJobExecutionManager() {

	}

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobExecutionManagerMessageConsumer.create(this);
		this.dhtCon = dhtConnectionProvider.owner(this.id).jobQueues(messageConsumer.jobs());
	}

	public static MRJobExecutionManager create(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider).taskExecutionScheduler(MinAssignedWorkersTaskExecutionScheduler.create())
				.maxNrOfExecutions(Runtime.getRuntime().availableProcessors());
	}

	public MRJobExecutionManager taskExecutionScheduler(ITaskScheduler taskExecutionScheduler) {
		this.taskExecutionScheduler = taskExecutionScheduler;
		return this;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtCon;
	}

	public MRJobExecutionManager maxNrOfExecutions(int maxNrOfExecutions) {
		this.maxNrOfExecutions = maxNrOfExecutions;
		return this;
	}
	// END GETTER/SETTER

	// Maintenance

	public void shutdown() {
		this.messageConsumer.canTake(false);
		dhtCon.shutdown();
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
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), outputPExecutor, outputPExecutable, outputPIndex);

		// Tries to find task keys on the network
		getTaskKeysFromNetwork(job, bcMessages, procedure, outputJPD);

		logger.info("Scheduling tasks for execution in case task was received from broadcast.");
		// In parallel to finding task keys on the network, execution starts in case a task was received from broadcast... Else just wait for the
		// broadcast or the dht call
		tryExecutingTask(job, bcMessages, procedure, outputJPD);

	}

	private void getTaskKeysFromNetwork(Job job, PriorityBlockingQueue<IBCMessage> bcMessages, Procedure procedure, JobProcedureDomain outputJPD) {
		logger.info("Retrieve task keys for input procedure domain: " + procedure.inputDomain() + ".");
		dhtCon.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, procedure.inputDomain().toString())
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							procedure.inputDomain().tasksSize(future.dataMap().size());
							for (Number640 keyHash : future.dataMap().keySet()) {
								String key = (String) future.dataMap().get(keyHash).object();
								logger.info("Found key: " + key);
								Task task = Task.create(key);
								if (!procedure.tasks().contains(task)) {// Don't need to add it more, got it e.g. from a BC
									logger.info("Added key " + key + ", scheduling task for execution.");
									procedure.tasks().add(task);
									tryExecutingTask(job, bcMessages, procedure, outputJPD);
								}
							}
						} else {
							logger.info("Failed to retrieve task keys for input job procedure domain: " + procedure.inputDomain());
						}
					}

				});
	}

	public void executeTask(PriorityBlockingQueue<IBCMessage> bcMessages, Task task, Procedure procedure, JobProcedureDomain outputJPD) {
		logger.info("Can execute? "+executionCounter +"<"+maxNrOfExecutions +"?"+canExecute());
		if (canExecute()) {
			this.executionCounter++;
			task.isActive(true);
			logger.info("Task to execute: " + task);
			// Now we actually wanna retrieve the data from the specified locations...
			dhtCon.getAll(task.key(), procedure.inputDomain().toString()).addListener(new BaseFutureAdapter<FutureGet>() {

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
						ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), id, task.nextStatusIndexFor(id), outputJPD);
						IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtCon);

						logger.info("Executing task: " + task.key() + " with values " + values);
						procedure.executable().process(task.key(), values, context);

						Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
							@Override
							public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
								if (future.isSuccess()) {
									outputETD.resultHash(context.resultHash());
									CompletedBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(
											outputETD.procedureIndex(procedure.procedureIndex()),
											procedure.inputDomain().nrOfFinishedTasks(procedure.nrOfFinishedTasks()));
									bcMessages.add(msg); // Adds it to itself, does not receive broadcasts... Makes sure this result is ignored in
															// case another was received already
									dhtCon.broadcastCompletion(msg);
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

	private void tryExecutingTask(Job job, PriorityBlockingQueue<IBCMessage> bcMessages, Procedure procedure, JobProcedureDomain toJPD) {
		if (procedure.tasks().size() != 0) {
			Task nextTask = taskExecutionScheduler.schedule(procedure.tasks());

			if (nextTask == null) {
				if (procedure.inputDomain().tasksSize() > procedure.tasks().size()) {
					logger.info("Do nothing, as there may come more tasks from dht or broadcast");
				} else if (procedure.inputDomain().tasksSize() == procedure.tasks().size()) {
					procedure.inputDomain().nrOfFinishedTasks(procedure.nrOfFinishedTasks());
					transferData(bcMessages, procedure.tasks(), toJPD, procedure.inputDomain());
				} else {// if (procedure.tasksSize() < procedure.tasks().size()) {
					logger.warn("Can only happen when a task is received from a broadcast first --> Set in broadcast (should have happend)");
				}
			} else {
				logger.info("Executing task: " + nextTask.key());
				nextTask.addAssignedExecutor(id);
				executeTask(bcMessages, nextTask, procedure, toJPD);
			}
		} else {
			logger.info("No tasks yet... Keep waiting until we receive something.");
		}
	}

	public void transferData(PriorityBlockingQueue<IBCMessage> bcMessages, List<Task> tasksToTransfer, JobProcedureDomain toJPD,
			JobProcedureDomain inputDomain) {
		logger.info("Finishing procedure");
		// Check if all tasks are transferred from their task domain to the corresponding procedure domain.
		List<Task> tasksToFinish = syncedArrayList();
		List<FutureGet> futureGetKeys = syncedArrayList();
		List<FutureGet> futureGetValues = syncedArrayList();
		List<FuturePut> futurePuts = syncedArrayList();
		for (Task task : tasksToTransfer) {
			if (task.isFinished() && !task.isInProcedureDomain()) {
				tasksToFinish.add(task);
				ExecutorTaskDomain fromETD = (ExecutorTaskDomain) task.resultOutputDomain();
				transferDataFromETDtoJPD(task, fromETD, toJPD, futureGetKeys, futureGetValues, futurePuts);
			} else {
				logger.info("Should not get here: tasks is not finished!");
			}
		}
		Futures.whenAllSuccess(futureGetKeys).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

			@Override
			public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
				if (future.isSuccess()) {
					Futures.whenAllSuccess(futureGetValues).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

						@Override
						public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
							if (future.isSuccess()) {
								Futures.whenAllSuccess(futurePuts).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

									@Override
									public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
										if (future.isSuccess()) {
											for (Task task : tasksToFinish) {
												task.isInProcedureDomain(true);
											}
											// Check again...
											boolean allTasksFinished = true;
											for (Task task : tasksToTransfer) {
												if (!task.isFinished()) {
													allTasksFinished = false;
												}
											}
											if (allTasksFinished) {
												CompletedBCMessage procedureCompleteBCMsg = CompletedBCMessage
														.createCompletedProcedureBCMessage(toJPD, inputDomain);
												bcMessages.add(procedureCompleteBCMsg);
												dhtCon.broadcastCompletion(procedureCompleteBCMsg);
											}
											logger.info("Successfully transfered task output keys and values for tasks " + tasksToFinish
													+ " from task executor domain to job procedure domain: " + toJPD.toString() + ". ");
										} else {
											logger.warn("Failed to transfered task output keys and values for task " + tasksToFinish
													+ " from task executor domain to job procedure domain: " + toJPD.toString() + ". failed reason: "
													+ future.failedReason());
										}
									}

								});
							} else {
								logger.warn("Failed to get task values for task " + tasksToFinish + " from task executor domain. failed reason: "
										+ future.failedReason());
							}
						}

					});
				} else {
					logger.warn("Failed to get task keys for task " + tasksToFinish + " from task executor domain. failed reason: "
							+ future.failedReason());
				}
			}

		});
	}

	private void transferDataFromETDtoJPD(Task task, ExecutorTaskDomain fromETD, JobProcedureDomain toJPD, List<FutureGet> futureGetKeys,
			List<FutureGet> futureGetValues, List<FuturePut> futurePuts) {

		futureGetKeys.add(dhtCon.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, fromETD.toString()).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					Set<Number640> keySet = future.dataMap().keySet();
					for (Number640 n : keySet) {
						String taskOutputKey = (String) future.dataMap().get(n).object();
						futureGetValues.add(dhtCon.getAll(taskOutputKey, fromETD.toString()).addListener(new BaseFutureAdapter<FutureGet>() {

							@Override
							public void operationComplete(FutureGet future) throws Exception {
								if (future.isSuccess()) {
									Collection<Data> values = future.dataMap().values();
									List<Object> realValues = syncedArrayList();
									for (Data d : values) {
										realValues.add(((Value) d.object()).value());
									}

									futurePuts.add(
											dhtCon.addAll(taskOutputKey, values, toJPD.toString()).addListener(new BaseFutureAdapter<FuturePut>() {

										@Override
										public void operationComplete(FuturePut future) throws Exception {

											if (future.isSuccess()) {
												logger.info("Successfully added task output values {" + realValues + "} of task output key \""
														+ taskOutputKey + "\" for task " + task.key() + " to output procedure domain "
														+ toJPD.toString());

											} else {
												logger.info(
														"Failed to add values for task output key " + taskOutputKey + " to output procedure domain "
																+ toJPD.toString() + ", failed reason: " + future.failedReason());
											}
										}

									}));
									futurePuts.add(dhtCon.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, taskOutputKey, toJPD.toString(), false)
											.addListener(new BaseFutureAdapter<FuturePut>() {

										@Override
										public void operationComplete(FuturePut future) throws Exception {
											if (future.isSuccess()) {
												logger.info("Successfully added task output key \"" + taskOutputKey + "\" for task " + task.key()
														+ " to output procedure domain " + toJPD.toString());
											} else {
												logger.info("Failed to add task output key and values for task output key \"" + taskOutputKey
														+ "\" for task " + task.key() + " to output procedure domain " + toJPD.toString()
														+ ", failed reason: " + future.failedReason());
											}
										}

									}));

								} else {
									logger.info("Failed to get task output key and values for task output key (" + taskOutputKey
											+ " from task executor domain " + fromETD.toString() + ", failed reason: " + future.failedReason());

								}
							}
						}));

					}
				} else {
					logger.warn("Failed to get task keys for task " + task.key() + " from task executor domain " + fromETD.toString()
							+ ", failed reason: " + future.failedReason());
				}
			}
		}));

	}

	public void abortExecution() {
		// TODO Auto-generated method stub

	}

	public boolean canExecute() {
		return this.executionCounter < this.maxNrOfExecutions;
	}

	// End Execution

}
