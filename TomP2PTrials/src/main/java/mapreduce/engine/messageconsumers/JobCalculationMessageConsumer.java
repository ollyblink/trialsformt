package mapreduce.engine.messageconsumers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.updates.IUpdate;
import mapreduce.engine.messageconsumers.updates.TaskUpdate;
import mapreduce.engine.messageconsumers.updates.ProcedureUpdate;
import mapreduce.engine.multithreading.PriorityExecutor;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number640;

public class JobCalculationMessageConsumer extends AbstractMessageConsumer {
	private static final int DEFAULT_NR_OF_THREADS = 1;

	private static Logger logger = LoggerFactory.getLogger(JobCalculationMessageConsumer.class);

	private PriorityExecutor threadPoolExecutor;

	private Map<String, Boolean> currentlyRetrievingTaskKeysForProcedure = SyncedCollectionProvider.syncedHashMap();

	private Map<String, ListMultimap<Task, Future<?>>> futures;

	private JobCalculationMessageConsumer(int maxThreads) {
		super();
		futures = SyncedCollectionProvider.syncedHashMap();
		this.threadPoolExecutor = PriorityExecutor.newFixedThreadPool(maxThreads);

	}

	public static JobCalculationMessageConsumer create(int nrOfThreads) {
		return new JobCalculationMessageConsumer(nrOfThreads);
	}

	public static JobCalculationMessageConsumer create() {
		return new JobCalculationMessageConsumer(DEFAULT_NR_OF_THREADS);
	}

	private void handleReceivedMessage(Job job, IDomain outputDomain, JobProcedureDomain inputDomain, IUpdate iUpdate) {

		if (job == null || outputDomain == null || inputDomain == null || iUpdate == null) {
			return;
		}
		Procedure procedure = job.currentProcedure();
		JobProcedureDomain receivedOutputProcedureDomain = (outputDomain instanceof JobProcedureDomain ? (JobProcedureDomain) outputDomain
				: ((ExecutorTaskDomain) outputDomain).jobProcedureDomain());
		if (procedure.procedureIndex() <= receivedOutputProcedureDomain.procedureIndex()) {
			if (procedure.procedureIndex() < receivedOutputProcedureDomain.procedureIndex()) {
				// Means this executor is behind in the execution than the one that sent this message
				cancelProcedureExecution(procedure);
				while (procedure.procedureIndex() < receivedOutputProcedureDomain.procedureIndex()) {
					job.incrementProcedureIndex();
				}
				procedure.dataInputDomain(inputDomain);
			} // no else needed... if it's the same procedure index, we are up to date and can update
			if (procedure.dataInputDomain().equals(inputDomain)) { // same procedure, same input data location: everything is fine!
				if (procedure.dataInputDomain().expectedNrOfFiles() < inputDomain.expectedNrOfFiles()) {// looks like the received had more already
					procedure.dataInputDomain().expectedNrOfFiles(inputDomain.expectedNrOfFiles());
				}
				procedure = iUpdate.executeUpdate(outputDomain, procedure);
			} else { // May have to change input data location (inputDomain)
				// executor of received message executes on different input data! Need to synchronize
				if (procedure.nrOfFinishedTasks() < inputDomain.nrOfFinishedTasks()) {
					// We have completed fewer tasks with our data set than the incoming... abort us and use the incoming data set location instead
					cancelProcedureExecution(procedure);
					procedure.dataInputDomain(inputDomain);
					// tryExecuting(procedure);
				} else if (procedure.nrOfFinishedTasks() == inputDomain.nrOfFinishedTasks()) { // What if they executed the same number of tasks?
					// TODO: What could it be? E.g. compare processor capabilities and take the one with the better ones as the faster will most
					// likely finish more tasks quicker
					logger.info("TODO: finished the same number of tasks with different data sets...\n"
							+ "What could it be? E.g. compare processor capabilities and take the one with the better ones as the faster will most likely finish more tasks quicker\n"
							+ "Or compare the tasks output values size");
				} // else{ ignore, as we are the ones that finished more already...
			}
			if (!job.isFinished()) {
				tryExecuting(procedure);
			}

		}
		// else if (receivedOutputProcedureDomain.procedureIndex() == 0) {// Start procedure... handle differently
		//
		// }
		// else{ ignore, as this is a message for an old procedure }
		//
	}

	private void tryExecuting(Procedure procedure) {
		if ((procedure.tasks().size() < procedure.dataInputDomain().expectedNrOfFiles() || procedure.tasks().size() == 0)
				&& procedure.procedureIndex() > 0) {
			// This means that there are still some tasks left in the dht and that it is currently not retrieving the tasks for this
			// procedure
			getTaskKeysFromNetwork(procedure);
		} else if (procedure.tasks().size() == procedure.dataInputDomain().expectedNrOfFiles()) {
			for (Task task : procedure.tasks()) {
				submitTask(procedure, task);
			}
		}
	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		handleReceivedMessage(job, outputDomain, inputDomain, new ProcedureUpdate(job, this));
	}

	@Override
	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {
		handleReceivedMessage(job, outputDomain, inputDomain, new TaskUpdate(this));
	}

	private void submitTask(Procedure procedure, Task task) {
		if (task.canBeExecuted()) {

			task.incrementActiveCount();
			addTaskFuture(procedure, task, threadPoolExecutor.submit(new Runnable() {

				@Override
				public void run() {
					executor().executeTask(task, procedure);
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

	public void cancelProcedureExecution(Procedure procedure) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(procedure.dataInputDomain().toString());
		if (procedureFutures != null) {
			for (Future<?> taskFuture : procedureFutures.values()) {
				taskFuture.cancel(true);
			}
		}
	}

	public void cancelTaskExecution(Procedure procedure, Task task) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(procedure.dataInputDomain().toString());
		if (procedureFutures != null) {
			List<Future<?>> taskFutures = procedureFutures.get(task);
			for (Future<?> taskFuture : taskFutures) {
				taskFuture.cancel(true);
			}
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
								procedure.dataInputDomain().expectedNrOfFiles(future.dataMap().size());
								for (Number640 keyHash : future.dataMap().keySet()) {
									String key = (String) future.dataMap().get(keyHash).object();
									Task task = Task.create(key);
									if (!procedure.tasks().contains(task)) {// Don't need to add it more, got it e.g. from a BC
										procedure.tasks().add(task);
									}
								}
								synchronized (procedure.tasks()) {
									Collections.shuffle(procedure.tasks());
								}

								for (Task task : procedure.tasks()) {
									submitTask(procedure, task);
								}

								currentlyRetrievingTaskKeysForProcedure.remove(procedure.dataInputDomain().toString());
							} else {
								logger.info("Fail reason: " + future.failedReason());
							}
						}

					});
		}
	}

	public void printResults(Job job) {
		// try {
		// Thread.sleep(2000);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		Procedure procedure = job.procedure(job.currentProcedure().procedureIndex() - 1);
		dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, procedure.resultOutputDomain().toString())
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							Set<Number640> keySet = future.dataMap().keySet();
							System.out.println("Found: " + keySet.size() + " finished tasks.");
							for (Number640 k : keySet) {
								String key = (String) future.dataMap().get(k).object();
								dhtConnectionProvider.getAll(key, procedure.resultOutputDomain().toString())
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

	@Override
	public JobCalculationExecutor executor() {
		return (JobCalculationExecutor) super.executor();
	}

	@Override
	public JobCalculationMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		return (JobCalculationMessageConsumer) super.dhtConnectionProvider(dhtConnectionProvider);
	}

	@Override
	public JobCalculationMessageConsumer executor(IExecutor executor) {
		return (JobCalculationMessageConsumer) super.executor(executor);
	}

}
