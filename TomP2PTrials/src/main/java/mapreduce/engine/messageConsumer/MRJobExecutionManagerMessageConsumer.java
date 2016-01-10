package mapreduce.engine.messageConsumer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import mapreduce.engine.broadcasting.IBCMessage;
import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.IDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.scheduling.ITaskScheduler;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number640;

public class MRJobExecutionManagerMessageConsumer extends AbstractMessageConsumer {

	/** Only used to distinguish if its a completed procedure or task to update */
	private interface IUpdate {
		public void executeUpdate(IDomain outputDomain, Procedure currentProcedure);
	}

	private MRJobExecutionManager jobExecutor;

	private ITaskScheduler taskExecutionScheduler;
	private int maxNrOfExecutions = 2;

	private ThreadPoolExecutor taskExecutionServer;

	/** Used to cancel all futures in case abort is used... All generated futures are stored here */
	private Map<Procedure, Multimap<Task, Future<?>>> executingTaskThreads = SyncedCollectionProvider.syncedHashMap();
	/** USed to cancel futures for retrieving data from dht */
	private ListMultimap<Procedure, Future<?>> retrieveTasksThreads = SyncedCollectionProvider.syncedListMultimap();

	private MRJobExecutionManagerMessageConsumer(MRJobExecutionManager jobExecutor) {
		this.jobExecutor = jobExecutor;
		this.taskExecutionServer = new ThreadPoolExecutor(maxNrOfExecutions, maxNrOfExecutions, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>());

	}

	public MRJobExecutionManagerMessageConsumer taskExecutionScheduler(ITaskScheduler taskExecutionScheduler) {
		this.taskExecutionScheduler = taskExecutionScheduler;
		return this;
	}

	public MRJobExecutionManagerMessageConsumer maxNrOfExecutions(int maxNrOfExecutions) {
		this.maxNrOfExecutions = maxNrOfExecutions;
		this.taskExecutionServer = new ThreadPoolExecutor(maxNrOfExecutions, maxNrOfExecutions, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>());
		return this;
	}

	public static MRJobExecutionManagerMessageConsumer create(MRJobExecutionManager jobExecutor) {
		return new MRJobExecutionManagerMessageConsumer(jobExecutor).taskExecutionScheduler(MinAssignedWorkersTaskExecutionScheduler.create());
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

	public boolean canExecute() {
		logger.info("this.taskExecutionServer.getActiveCount() < this.maxNrOfExecutions? " + this.taskExecutionServer.getActiveCount() + " < "
				+ this.maxNrOfExecutions);
		return this.taskExecutionServer.getActiveCount() < this.maxNrOfExecutions;
	}

	private void handleReceivedMessage(Job job, IDomain outputDomain, JobProcedureDomain inputDomain, IUpdate iUpdate) {
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
				procedure.inputDomain(inputDomain);
			} // no else needed... if it's the same procedure index, we are up to date and can update
			if (procedure.inputDomain().equals(inputDomain)) { // same procedure, same input data location: everything is fine!
				logger.info("tasks sizes: here: " + procedure.inputDomain().tasksSize() + " < as there " + inputDomain.tasksSize());
				if (procedure.inputDomain().tasksSize() < inputDomain.tasksSize()) {// looks like the received had more already
					procedure.inputDomain().tasksSize(inputDomain.tasksSize());
				}
				iUpdate.executeUpdate(outputDomain, procedure);
			} else { // May have to change input data location (inputDomain)
				// executor of received message executes on different input data! Need to synchronize
				if (procedure.nrOfFinishedTasks() < inputDomain.nrOfFinishedTasks()) {// Simply compare which one has finished more tasks so far and
																						// take that one
					// We have completed fewer tasks with our data set than the incoming... abort us and use the incoming data set location instead
					cancelProcedureExecution(procedure);
					procedure.inputDomain(inputDomain);
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

		logger.info("Execute next");
		executeNext();
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
					job.currentProcedure().inputDomain(outputJPD);
					if (job.currentProcedure().executable().getClass().getSimpleName().equals(EndProcedure.class.getSimpleName())) {
						job.isFinished(true);
					}
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
					task = tasks.get(tasks.indexOf(task)).addAssignedExecutor(outputETDomain.executor());
				}
				if (!task.isFinished()) {// Is finished before adding new output procedure domain? then ignore update
					task.addOutputDomain(outputETDomain);
					// Is finished after adding new output procedure domain? then abort any executions of this task and transfer the task's output
					// <K,{V}> to the procedure domain
					if (task.isFinished()) {
						cancelTaskExecution(procedure, task); // If so, no execution needed anymore
						// Transfer data to procedure domain! This may cause the procedure to become finished
						jobExecutor.switchDataFromTaskToProcedureDomain(procedure, task);
					}
				}
			}

		});

	}

	private void getTaskKeysFromNetwork(Procedure procedure) {
		logger.info("Retrieving tasks for: " + procedure.inputDomain().toString());
		jobExecutor.dhtConnectionProvider().getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, procedure.inputDomain().toString())
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							procedure.inputDomain().tasksSize(future.dataMap().size());
							for (Number640 keyHash : future.dataMap().keySet()) {
								String key = (String) future.dataMap().get(keyHash).object();
								Task task = Task.create(key);
								if (!procedure.tasks().contains(task)) {// Don't need to add it more, got it e.g. from a BC
									procedure.tasks().add(task);
									logger.info("added task " + task);
									executeNext();
								}
							}
						}
					}

				});
	}

	private void executeNext() {
		Job job = jobs.firstKey();
		logger.info("Try to execute job " + job);
		if (!job.isFinished()) {
			Procedure procedure = job.currentProcedure();
			logger.info("Tasks: " + procedure.tasks());
			Task task = taskExecutionScheduler.schedule(procedure.tasks());
			if (task == null) {
				logger.info("Finished all current tasks... maybe more will come in with broadcast or from dht ");
				if (procedure.tasks().size() == 0 || procedure.tasks().size() < procedure.inputDomain().tasksSize()) {
					logger.info("Retrieve tasks from dht/local storage. Can execute? " + canExecute());
					if (canExecute()) {
						Runnable retrieveDataThread = new Runnable() {

							@Override
							public void run() {
								logger.info("In runnable");
								getTaskKeysFromNetwork(procedure);
							}
						};

						this.retrieveTasksThreads.put(procedure, taskExecutionServer.submit(retrieveDataThread));
					}
				}
			} else {
				logger.info("Executing next task: " + task);
				if (canExecute()) {
					Multimap<Task, Future<?>> multimap = executingTaskThreads.get(procedure);
					if (multimap == null) {
						multimap = Multimaps.synchronizedMultimap(ArrayListMultimap.create());
						executingTaskThreads.put(procedure, multimap);
					}
					Runnable taskExecution = new Runnable() {

						@Override
						public void run() {
							jobExecutor.executeTask(task, procedure);
						}

					};

					multimap.put(task, taskExecutionServer.submit(taskExecution));
				}
			}
		} else {
			logger.info("No job to execute...");
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Procedure procedure = job.procedure(2);
			jobExecutor.dhtConnectionProvider().getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, procedure.resultOutputDomain().toString())
					.addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								Set<Number640> keySet = future.dataMap().keySet();
								for (Number640 k : keySet) {
									String key = (String) future.dataMap().get(k).object();
									jobExecutor.dhtConnectionProvider().getAll(key, procedure.resultOutputDomain().toString())
											.addListener(new BaseFutureAdapter<FutureGet>() {

										@Override
										public void operationComplete(FutureGet future) throws Exception {
											if (future.isSuccess()) {
												Set<Number640> keySet2 = future.dataMap().keySet();
												String values = "";
												for (Number640 k2 : keySet2) {
													values += ((Value) future.dataMap().get(k2).object()).value() + ", ";
												}
												System.err.println(key + ":" + values);
											}
										}

									});
								}
							}
						}

					});
		}

	}

	private void cancelProcedureExecution(Procedure procedure) {
		logger.info("Cancelled procedure execution");
		Multimap<Task, Future<?>> multimap = executingTaskThreads.get(procedure);
		if (multimap != null) {
			for (Future<?> future : multimap.values()) {
				future.cancel(true);
			}
			executingTaskThreads.get(procedure).clear();
		}
		List<Future<?>> dataRetrievalFutures = retrieveTasksThreads.get(procedure);
		for (Future<?> future : dataRetrievalFutures) {
			future.cancel(true);
		}
		retrieveTasksThreads.get(procedure).clear();
		// taskExecutionServer.shutdown();
		// this.taskExecutionServer = new ThreadPoolExecutor(maxNrOfExecutions, maxNrOfExecutions, 0L, TimeUnit.MILLISECONDS,
		// new LinkedBlockingQueue<Runnable>());
	}

	private void cancelTaskExecution(Procedure procedure, Task task) {
		logger.info("Cancelled task execution");
		Multimap<Task, Future<?>> multimap = executingTaskThreads.get(procedure);
		if (multimap != null) {
			Collection<Future<?>> allTaskExecutions = multimap.get(task);
			for (Future<?> future : allTaskExecutions) {
				future.cancel(true);
			}
			multimap.get(task).clear();
			// taskExecutionServer.shutdown();
			// this.taskExecutionServer = new ThreadPoolExecutor(maxNrOfExecutions, maxNrOfExecutions, 0L, TimeUnit.MILLISECONDS,
			// new LinkedBlockingQueue<Runnable>());
		}
	}
}
