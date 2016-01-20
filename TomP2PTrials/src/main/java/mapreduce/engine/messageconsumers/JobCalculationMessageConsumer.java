package mapreduce.engine.messageconsumers;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.engine.messageconsumers.updates.IUpdate;
import mapreduce.engine.messageconsumers.updates.ProcedureUpdate;
import mapreduce.engine.messageconsumers.updates.TaskUpdate;
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
import mapreduce.utils.resultprinter.DefaultResultPrinter;
import mapreduce.utils.resultprinter.IResultPrinter;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number640;

public class JobCalculationMessageConsumer extends AbstractMessageConsumer {
	private static final int DEFAULT_NR_OF_THREADS = 1;

	private static Logger logger = LoggerFactory.getLogger(JobCalculationMessageConsumer.class);

	private PriorityExecutor threadPoolExecutor;

	private Map<String, Boolean> currentlyRetrievingTaskKeysForProcedure = SyncedCollectionProvider.syncedHashMap();

	private Map<String, ListMultimap<Task, Future<?>>> futures = SyncedCollectionProvider.syncedHashMap();

	private Comparator<PerformanceInfo> performanceEvaluator = new Comparator<PerformanceInfo>() {

		@Override
		public int compare(PerformanceInfo o1, PerformanceInfo o2) {
			// TODO: finished the same number of tasks with different data sets... What could it be? E.g. compare processor capabilities and take
			// the one with the better ones as the faster will most likely finish more tasks quicker. Or compare the tasks output values size
			return 1;
		}

	};

	private IResultPrinter resultPrinter = DefaultResultPrinter.create();

	private JobCalculationMessageConsumer(int maxThreads) {
		super();
		this.threadPoolExecutor = PriorityExecutor.newFixedThreadPool(maxThreads);

	}

	public static JobCalculationMessageConsumer create(int nrOfThreads) {
		return new JobCalculationMessageConsumer(nrOfThreads);
	}

	public static JobCalculationMessageConsumer create() {
		return new JobCalculationMessageConsumer(DEFAULT_NR_OF_THREADS);
	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		handleReceivedMessage(job, outputDomain, inputDomain, new ProcedureUpdate(job, this));
	}

	@Override
	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {
		handleReceivedMessage(job, outputDomain, inputDomain, new TaskUpdate(this));
	}

	private void handleReceivedMessage(Job job, IDomain outputDomain, JobProcedureDomain inputDomain, IUpdate iUpdate) {
		if (job == null || outputDomain == null || inputDomain == null || iUpdate == null) {
			return;
		}
		JobProcedureDomain rJPD = (outputDomain instanceof JobProcedureDomain ? (JobProcedureDomain) outputDomain
				: ((ExecutorTaskDomain) outputDomain).jobProcedureDomain());

		boolean receivedOutdatedMessage = job.currentProcedure().procedureIndex() > rJPD.procedureIndex();
		if (receivedOutdatedMessage) {
			logger.info("Received an old message: nothing to do.");
			return;
		} else {
			// need to increment procedure because we are behind in execution?
			tryIncrementProcedure(job, inputDomain, rJPD);
			// Same input data? Then we may try to update tasks/procedures
			tryUpdateTasksOrProcedures(job, inputDomain, outputDomain, iUpdate);
			// Anything left to execute for this procedure?
			tryExecuteProcedure(job);
		}
	}

	private void tryIncrementProcedure(Job job, JobProcedureDomain dataInputDomain, JobProcedureDomain rJPD) {
		int currentProcIndex = job.currentProcedure().procedureIndex();
		int receivedPIndex = rJPD.procedureIndex();
		if (currentProcIndex < receivedPIndex) {
			// Means this executor is behind in the execution than the one that sent this message --> increment until we are up to date again
			cancelProcedureExecution(job.currentProcedure());
			while (job.currentProcedure().procedureIndex() < receivedPIndex) {
				job.incrementProcedureIndex();
			}
			job.currentProcedure().dataInputDomain(dataInputDomain);
		} // no else needed... if it's the same procedure index, we are up to date and can try to update
	}

	private void tryUpdateTasksOrProcedures(Job job, JobProcedureDomain inputDomain, IDomain outputDomain, IUpdate iUpdate) {
		Procedure procedure = job.currentProcedure();
		if (procedure.dataInputDomain().equals(inputDomain)) { // same procedure, same input data location: everything is fine!
			if (procedure.dataInputDomain().expectedNrOfFiles() < inputDomain.expectedNrOfFiles()) {// looks like the received had more already
				procedure.dataInputDomain().expectedNrOfFiles(inputDomain.expectedNrOfFiles());
			}
			iUpdate.executeUpdate(outputDomain, procedure); // Only here: execute the received task/procedure update
		} else { // May have to change input data location (inputDomain)
			// executor of received message executes on different input data! Need to synchronize
			changeDataInputDomain(inputDomain, procedure);
		}
	}

	private void changeDataInputDomain(JobProcedureDomain inputDomain, Procedure procedure) {
		if (procedure.dataInputDomain().nrOfFinishedTasks() < inputDomain.nrOfFinishedTasks()) {
			// We have completed fewer tasks with our data set than the incoming... abort us and use the incoming data set location instead
			cancelCurrentExecutionsAndUpdateDataInputDomain(inputDomain, procedure);
		} else {
			boolean haveExecutedTheSameNrOfTasks = procedure.dataInputDomain().nrOfFinishedTasks() == inputDomain.nrOfFinishedTasks();
			if (haveExecutedTheSameNrOfTasks) {
				int comparisonResult = this.performanceEvaluator.compare(executor.performanceInformation(),
						inputDomain.executorPerformanceInformation());
				boolean thisExecutorHasWorsePerformance = comparisonResult == -1;
				if (thisExecutorHasWorsePerformance) {
					// we are expected to finish later due to worse performance --> abort this one's execution
					cancelCurrentExecutionsAndUpdateDataInputDomain(inputDomain, procedure);
				}
			}
		} // else{ ignore, as we are the ones that finished more already...
	}

	private void cancelCurrentExecutionsAndUpdateDataInputDomain(JobProcedureDomain inputDomain, Procedure procedure) {
		cancelProcedureExecution(procedure);
		procedure.dataInputDomain(inputDomain);
	}

	private void tryExecuteProcedure(Job job) {
		Procedure procedure = job.currentProcedure();
		if (!job.isFinished()) {
			boolean isNotComplete = procedure.tasksSize() < procedure.dataInputDomain().expectedNrOfFiles();
			boolean isNotStartProcedure = procedure.procedureIndex() > 0;
			if (isNotComplete && isNotStartProcedure) {
				tryRetrieveMoreTasksFromDHT(procedure);
			} else {
				boolean isExpectedToBeComplete = procedure.tasksSize() == procedure.dataInputDomain().expectedNrOfFiles();
				if (isExpectedToBeComplete) {
					trySubmitTasks(procedure);
				}
			}
		} else {// if(job.isFinished()){
			resultPrinter.printResults(dhtConnectionProvider, procedure.resultOutputDomain().toString());
		}
	}

	private void tryRetrieveMoreTasksFromDHT(Procedure procedure) {
		JobProcedureDomain dataInputDomain = procedure.dataInputDomain();
		Boolean retrieving = currentlyRetrievingTaskKeysForProcedure.get(dataInputDomain.toString());
		if ((retrieving == null || !retrieving)) { // This makes sure that if it is concurrently executed, retrieval is only once called...
			logger.info("Retrieving tasks for: " + dataInputDomain.toString());
			currentlyRetrievingTaskKeysForProcedure.put(dataInputDomain.toString(), true);
			dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, dataInputDomain.toString())
					.addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								int actualNrOfTasks = future.dataMap().size();
								logger.info("retrieved " + actualNrOfTasks + " tasks from dataInputDomain: " + dataInputDomain.toString());
								dataInputDomain.expectedNrOfFiles(actualNrOfTasks);
								for (Number640 keyHash : future.dataMap().keySet()) {
									String key = (String) future.dataMap().get(keyHash).object();
									Task task = Task.create(key, executor.id());
									procedure.addTask(task);
								}
								trySubmitTasks(procedure);
								currentlyRetrievingTaskKeysForProcedure.remove(dataInputDomain.toString());
							} else {
								logger.info("Fail reason: " + future.failedReason());
							}
						}
					});
		}
	}

	private void trySubmitTasks(Procedure procedure) {
		procedure.shuffleTasks();
		Task task = null;
		while ((task = procedure.nextExecutableTask()) != null) {
			addTaskFuture(procedure, task, threadPoolExecutor.submit(createTaskExecutionRunnable(procedure, task), task));
		}
	}

	private Runnable createTaskExecutionRunnable(Procedure procedure, Task task) {
		return new Runnable() {
			@Override
			public void run() {
				executor().executeTask(task, procedure);
			}
		};
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
			procedureFutures.clear();
		}
	}

	public void cancelTaskExecution(Procedure procedure, Task task) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(procedure.dataInputDomain().toString());
		if (procedureFutures != null) {
			List<Future<?>> taskFutures = procedureFutures.get(task);
			for (Future<?> taskFuture : taskFutures) {
				taskFuture.cancel(true);
			}
			procedureFutures.get(task).clear();
		}
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

	public JobCalculationMessageConsumer resultPrinter(IResultPrinter resultPrinter) {
		this.resultPrinter = resultPrinter;
		return this;
	}

	public JobCalculationMessageConsumer performanceEvaluator(Comparator<PerformanceInfo> performanceEvaluator) {
		this.performanceEvaluator = performanceEvaluator;
		return this;
	}
}
