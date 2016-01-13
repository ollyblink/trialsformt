package mapreduce.engine.messageconsumer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.engine.priorityexecutor.PriorityExecutor;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.IDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class MRJobExecutionManagerMessageConsumer implements IMessageConsumer {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManagerMessageConsumer.class);

	/** Only used to distinguish if its a completed procedure or task to update */
	private interface IUpdate {
		public void executeUpdate(IDomain outputDomain, Procedure currentProcedure);
	}

	private MRJobExecutionManager jobExecutor;

	private PriorityExecutor threadPoolExecutor;

	private int maxThreads = 8;

	private Map<String, Boolean> currentlyRetrievingTaskKeysForProcedure = SyncedCollectionProvider.syncedHashMap();

	private Map<String, ListMultimap<Task, Future<?>>> futures;

	private IDHTConnectionProvider dhtConnectionProvider;

	private MRJobExecutionManagerMessageConsumer(MRJobExecutionManager jobExecutor) {
		this.jobExecutor = jobExecutor;
		futures = SyncedCollectionProvider.syncedHashMap();
		this.threadPoolExecutor = PriorityExecutor.newFixedThreadPool(maxThreads);

	}

	// public MRJobExecutionManagerMessageConsumer taskExecutionScheduler(ITaskScheduler taskExecutionScheduler) {
	// this.taskExecutionScheduler = taskExecutionScheduler;
	// return this;
	// }

	public static MRJobExecutionManagerMessageConsumer create() {
		return new MRJobExecutionManagerMessageConsumer(MRJobExecutionManager.create());
		// .taskExecutionScheduler(MinAssignedWorkersTaskExecutionScheduler.create());
	}

	@Override
	public MRJobExecutionManagerMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		this.jobExecutor.dhtConnectionProvider(dhtConnectionProvider);
		return this;
	}

	// Maintenance

	public void start() {
		// this.dhtConnectionProvider.connect();
	}

	public void shutdown() {
		this.dhtConnectionProvider.shutdown();
	}

	// End Maintenance
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

	// @Override
	// public MRJobExecutionManagerMessageConsumer canTake(boolean canTake) {
	// return (MRJobExecutionManagerMessageConsumer) super.canTake(canTake);
	// }

	private void handleReceivedMessage(Job job, IDomain outputDomain, JobProcedureDomain inputDomain, IUpdate iUpdate) {

		if (job == null || outputDomain == null || inputDomain == null || iUpdate == null) {
			return;
		}
		Procedure procedure = job.currentProcedure();
		JobProcedureDomain thisOutputProcedureDomain = (outputDomain instanceof JobProcedureDomain ? (JobProcedureDomain) outputDomain
				: ((ExecutorTaskDomain) outputDomain).jobProcedureDomain());
		if (procedure.procedureIndex() <= thisOutputProcedureDomain.procedureIndex()) {
			if (procedure.procedureIndex() < thisOutputProcedureDomain.procedureIndex()) {
				// Means this executor is behind in the execution than the one that sent this message
				cancelProcedureExecution(procedure);
				while (procedure.procedureIndex() < thisOutputProcedureDomain.procedureIndex()) {
					job.incrementProcedureIndex();
				}
				procedure.dataInputDomain(inputDomain);
			} // no else needed... if it's the same procedure index, we are up to date and can update
			if (procedure.dataInputDomain().equals(inputDomain)) { // same procedure, same input data location: everything is fine!
				logger.info("tasks sizes: here: " + procedure.dataInputDomain().tasksSize() + " < as there " + inputDomain.tasksSize());
				if (procedure.dataInputDomain().tasksSize() < inputDomain.tasksSize()) {// looks like the received had more already
					procedure.dataInputDomain().tasksSize(inputDomain.tasksSize());
				}
				iUpdate.executeUpdate(outputDomain, procedure);
			} else { // May have to change input data location (inputDomain)
				// executor of received message executes on different input data! Need to synchronize
				if (procedure.nrOfFinishedTasks() < inputDomain.nrOfFinishedTasks()) {
					// We have completed fewer tasks with our data set than the incoming... abort us and use the incoming data set location instead
					cancelProcedureExecution(procedure);
					procedure.dataInputDomain(inputDomain);
					tryExecuting(procedure);
				} else if (procedure.nrOfFinishedTasks() == inputDomain.nrOfFinishedTasks()) { // What if they executed the same number of tasks?
					// TODO: What could it be? E.g. compare processor capabilities and take the one with the better ones as the faster will most
					// likely finish more tasks quicker
					logger.info("TODO: finished the same number of tasks with different data sets...\n"
							+ "What could it be? E.g. compare processor capabilities and take the one with the better ones as the faster will most likely finish more tasks quicker\n"
							+ "Or compare the tasks output values size");
				} // else{ ignore, as we are the ones that finished more already...
			}
		}
		// else{ ignore, as this is a message for an old procedure }
		//
	}

	private void tryExecuting(Procedure procedure) {
		// if (!job.isFinished()) {
		// JobProcedureDomain outputJPD = (outputDomain instanceof JobProcedureDomain ? ((JobProcedureDomain) outputDomain)
		// : ((ExecutorTaskDomain) outputDomain).jobProcedureDomain());
		if (procedure.tasks().size() < procedure.dataInputDomain().tasksSize() || procedure.tasks().size() == 0) {
			// This means that there are still some tasks left in the dht and that it is currently not retrieving the tasks for this
			// procedure
			getTaskKeysFromNetwork(procedure);
		} else if (procedure.tasks().size() == procedure.dataInputDomain().tasksSize()) {
			for (Task task : procedure.tasks()) {
				submitTask(procedure, task);
			}
		}
		// }
	}

	private void submitTask(Procedure procedure, Task task) {
		if (task.canBeExecuted()) {
			task.incrementActiveCount();
			addTaskFuture(procedure, task, threadPoolExecutor.submit(new Runnable() {

				@Override
				public void run() {
					jobExecutor.executeTask(task, procedure);
				}

			}, task));
		}
	}

	private void addTaskFuture(Procedure procedure, Task task, Future<?> taskFuture) {
		ListMultimap<Task, Future<?>> taskFutures = futures.get(procedure.dataInputDomain().toString());
		if (taskFutures == null) {
			taskFutures = SyncedCollectionProvider.syncedArrayListMultimap();
			futures.put(procedure.dataInputDomain().toString(), taskFutures);
		}
		taskFutures.put(task, taskFuture);
	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		handleReceivedMessage(job, outputDomain, inputDomain, new IUpdate() {
			@Override
			public void executeUpdate(IDomain outputDomain, Procedure procedure) {
				JobProcedureDomain outputJPD = (JobProcedureDomain) outputDomain;
				procedure.addOutputDomain(outputJPD);
				if (procedure.isFinished()) {
					cancelProcedureExecution(procedure);
					job.incrementProcedureIndex();
					job.currentProcedure().dataInputDomain(outputJPD);
					if (job.currentProcedure().executable().getClass().getSimpleName().equals(EndProcedure.class.getSimpleName())) {
						job.isFinished(true);
						printResults(job);
						return; // Done
					}else{
						procedure = job.currentProcedure();
					}
				}
				if (!job.isFinished()) {
					tryExecuting(procedure);
				}

			}
		});
	}

	@Override
	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {
		handleReceivedMessage(job, outputDomain, inputDomain, new IUpdate() {

			@Override
			public void executeUpdate(IDomain outputDomain, Procedure procedure) {
				ExecutorTaskDomain outputETDomain = (ExecutorTaskDomain) outputDomain;
				Task receivedTask = Task.create(outputETDomain.taskId());
				List<Task> tasks = procedure.tasks();
				Task task = receivedTask;
				if (!tasks.contains(task)) {
					procedure.addTask(task);
				} else {
					task = tasks.get(tasks.indexOf(task));
				}
				if (!task.isFinished()) {// Is finished before adding new output procedure domain? then ignore update
					task.addOutputDomain(outputETDomain);
					task.decrementActiveCount(); // TODO this one looks like its at the wrong position... rethink
					if (task.isFinished()) {// Is finished after adding new output procedure domain? then abort any executions of this task and
											// transfer the task's output <K,{V}> to the procedure domain
						cancelTaskExecution(procedure, task); // If so, no execution needed anymore
						// Transfer data to procedure domain! This may cause the procedure to become finished
						jobExecutor.switchDataFromTaskToProcedureDomain(procedure, task);
					}
				}
			}
		});
	}

	protected void cancelProcedureExecution(Procedure procedure) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(procedure.dataInputDomain().toString());
		if (procedureFutures != null) {
			for (Future<?> taskFuture : procedureFutures.values()) {
				taskFuture.cancel(true);
			}
		}
	}

	protected void cancelTaskExecution(Procedure procedure, Task task) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(procedure.dataInputDomain().toString());
		List<Future<?>> taskFutures = procedureFutures.get(task);
		for (Future<?> taskFuture : taskFutures) {
			taskFuture.cancel(true);
		}
	}

	private void getTaskKeysFromNetwork(Procedure procedure) {

		Boolean retrieving = currentlyRetrievingTaskKeysForProcedure.get(procedure.dataInputDomain().toString());
		if ((retrieving == null || !retrieving)) {
			logger.info("Retrieving tasks for: " + procedure.dataInputDomain().toString());
			currentlyRetrievingTaskKeysForProcedure.put(procedure.dataInputDomain().toString(), true);

			dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, procedure.dataInputDomain().toString())
					.addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								logger.info("Success");
								procedure.dataInputDomain().tasksSize(future.dataMap().size());
								for (Number640 keyHash : future.dataMap().keySet()) {
									String key = (String) future.dataMap().get(keyHash).object();
									Task task = Task.create(key);
									if (!procedure.tasks().contains(task)) {// Don't need to add it more, got it e.g. from a BC
										procedure.tasks().add(task);
										logger.info("added task " + task);
										submitTask(procedure, task);
									}
									currentlyRetrievingTaskKeysForProcedure.remove(procedure.dataInputDomain().toString());
								}
							} else {
								logger.info("Fail reason: " + future.failedReason());
							}
						}

					});
		}
	}

	private void printResults(Job job) {
//		try {
//			Thread.sleep(2000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		Procedure procedure = job.procedure(job.currentProcedure().procedureIndex()-1);
		dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, procedure.resultOutputDomain().toString())
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							Set<Number640> keySet = future.dataMap().keySet();
							System.out.println("Found: " +keySet.size() + " finished tasks.");
//							for (Number640 k : keySet) {
//								String key = (String) future.dataMap().get(k).object();
//								dhtConnectionProvider.getAll(key, procedure.resultOutputDomain().toString())
//										.addListener(new BaseFutureAdapter<FutureGet>() {
//
//									@Override
//									public void operationComplete(FutureGet future) throws Exception {
//										if (future.isSuccess()) {
//											Set<Number640> keySet2 = future.dataMap().keySet();
//											String values = "";
//											for (Number640 k2 : keySet2) {
//												values += ((Value) future.dataMap().get(k2).object()).value() + ", ";
//											}
//											System.err.println(key + ":" + values);
//										}
//									}
//
//								});
//							}
						}
					}

				});
	}

	public MRJobExecutionManager jobExecutor() {
		return this.jobExecutor;
	}

}
