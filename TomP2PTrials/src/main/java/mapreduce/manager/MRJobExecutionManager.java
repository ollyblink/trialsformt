package mapreduce.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.computation.context.PseudoStorageContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.scheduling.ITaskScheduler;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.execution.task.taskexecutor.ITaskExecutor;
import mapreduce.execution.task.taskexecutor.ParallelTaskExecutor;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;
import static mapreduce.utils.SyncedCollectionProvider.*;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	private static final List<Job> DEFAULT_JOB_LIST = Collections.synchronizedList(new ArrayList<>());
	private static final ITaskExecutor DEFAULT_TASK_EXCECUTOR = ParallelTaskExecutor.newInstance();
	private static final ITaskScheduler DEFAULT_TASK_EXECUTION_SCHEDULER = MinAssignedWorkersTaskExecutionScheduler.newInstance();
	private static final IContext DEFAULT_CONTEXT = PseudoStorageContext.newInstance();

	private IDHTConnectionProvider dhtConnectionProvider;
	private IContext context;
	private ITaskScheduler taskExecutionScheduler;
	private ITaskExecutor taskExecutor;
	private List<Job> jobs;
	private MRJobExecutionManagerMessageConsumer messageConsumer;

	private Future<?> createTaskThread;

	private ThreadPoolExecutor server;

	private String id;

	private Job currentlyExecutedJob;

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider, List<Job> jobs) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.dhtConnectionProvider(dhtConnectionProvider.owner(this.id));
		this.messageConsumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(this).canTake(true);
		this.dhtConnectionProvider().addMessageQueueToBroadcastHandlers(messageConsumer.queue());

		this.jobs = jobs;
		new Thread(messageConsumer).start();
	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider, DEFAULT_JOB_LIST).taskExecutor(DEFAULT_TASK_EXCECUTOR)
				.taskExecutionScheduler(DEFAULT_TASK_EXECUTION_SCHEDULER).context(DEFAULT_CONTEXT);
	}

	public Job currentlyExecutedJob() {
		return this.currentlyExecutedJob;
	}

	public MRJobExecutionManager taskExecutor(ITaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
		return this;
	}

	private MRJobExecutionManager dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public MRJobExecutionManager taskExecutionScheduler(ITaskScheduler taskExecutionScheduler) {
		this.taskExecutionScheduler = taskExecutionScheduler;
		return this;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public MRJobExecutionManager context(IContext context) {
		this.context = context;
		return this;
	}

	// END GETTER/SETTER

	// Maintenance

	public void shutdown() {
		dhtConnectionProvider.shutdown();
	}

	public void start() {
		dhtConnectionProvider.connect();

	}

	// End Maintenance
	// Execution

	public void executeJob(Job job) {
		this.currentlyExecutedJob = job;
		ProcedureInformation procedureInformation = currentlyExecutedJob.currentProcedure();
		logger.info("Got job: " + currentlyExecutedJob.id() + ", starting procedure " + procedureInformation);

		// Get the data for the job's current procedure
		this.server = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		List<Task> tasks = procedureInformation.tasks();
		String jobProcedureDomain = DomainProvider.INSTANCE.jobProcedureDomain(currentlyExecutedJob);
		this.createTaskThread = server.submit(new Runnable() { // makes sense as tasks may also be returned from outside (other executors), not only
																// from stored data

			@Override
			public void run() {
				// Get all procedure keys!! Create all the tasks for each key!!!
				dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_KEYS, jobProcedureDomain)
						.addListener(createTasksForProcedure(currentlyExecutedJob.id(), tasks, jobProcedureDomain));
			}
		});

		Task task = null;
		taskExecutionScheduler.procedureInformation(procedureInformation);
		while ((task = taskExecutionScheduler.schedule(tasks)) != null && !taskExecutor.abortedTaskExecution()) {
			final Task taskToDistribute = task;
			List<String> finalDataLocations = task.finalDataLocationDomains();
			List<Object> valuesCollector = syncedArrayList();
			List<FutureGet> futureGets = syncedArrayList();

			// TODO build in that the data retrieval may take a certain number of repetitions when failed before being broadcasted as failed
			for (int i = 0; i < finalDataLocations.size(); ++i) {
				dhtConnectionProvider.broadcastExecutingTask(task);
				// Now we actually wanna retrieve the data from the specified locations...
				futureGets.add(retrieveDataForTask(task, finalDataLocations.get(i), valuesCollector));
			}
			Futures.whenAllSuccess(futureGets)
					.addListener(executeTaskOnSuccessfulDataRetrieval(currentlyExecutedJob, taskToDistribute, valuesCollector));
		}

		if (procedureInformation.isFinished()) { // May have been aborted from outside and thus, hasn't finished yet (in case abortExecution(job) was
													// called
			dhtConnectionProvider.broadcastFinishedAllTasksOfProcedure(job);
		}

	}

	protected BaseFutureListener<FutureGet> createTasksForProcedure(String jobId, List<Task> tasks, String jobProcedureDomain) {
		return new BaseFutureListener<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					try {
						if (future.dataMap() != null) {
							for (Number640 n : future.dataMap().keySet()) {
								Object key = future.dataMap().get(n).object();
								Task task = Task.newInstance(key, jobId);
								if (tasks.contains(task)) {// Don't need to add it more, got it e.g. from a BC
									return;
								} else {
									dhtConnectionProvider.getAll(key.toString(), jobProcedureDomain).addListener(new BaseFutureListener<FutureGet>() {

										@Override
										public void operationComplete(FutureGet future) throws Exception {
											if (future.isSuccess()) {
												try {
													if (future.dataMap() != null) {
														for (Number640 n : future.dataMap().keySet()) {
															Object taskExecutorDomain = future.dataMap().get(n).object();
															task.finalDataLocationDomains(taskExecutorDomain.toString());
														}
														if (!tasks.contains(task)) {
															tasks.add(task);
														}
													}
												} catch (IOException e) {
													// dhtConnectionProvider.broadcastFailedTask(taskToDistribute);
												}
											} else {
												// dhtConnectionProvider.broadcastFailedJob(jobs.get(0));
											}
										}

										@Override
										public void exceptionCaught(Throwable t) throws Exception {
											logger.warn("Exception thrown", t);
											// dhtConnectionProvider.broadcastFailedJob(jobs.get(0));
										}
									});
								}
							}
						}
					} catch (IOException e) {
						// dhtConnectionProvider.broadcastFailedTask(taskToDistribute);
					}
				} else {
					// dhtConnectionProvider.broadcastFailedJob(jobs.get(0));
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.warn("Exception thrown", t);
				// dhtConnectionProvider.broadcastFailedJob(job);
			}
		};
	}

	private BaseFutureAdapter<FutureDone<FutureGet[]>> executeTaskOnSuccessfulDataRetrieval(Job job, final Task taskToDistribute,
			List<Object> valuesCollector) {
		return new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
			@Override
			public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
				String subsequentJobProcedureDomain = job.subsequentJobProcedureDomain();

				context.task(taskToDistribute).subsequentJobProcedureDomain(subsequentJobProcedureDomain);
				taskExecutor.execute(job.currentProcedure().procedure(), taskToDistribute.id(), valuesCollector, context);// Non-blocking!
			}

		};
	}

	private FutureGet retrieveDataForTask(Task task, String taskExecutorDomain, List<Object> valuesCollector) {
		return dhtConnectionProvider.getAll(task.id(), taskExecutorDomain).addListener(new BaseFutureListener<FutureGet>() {

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
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.warn("Exception on getting the data", t);
			}
		});
	}

	public void abortExecution(Job job) {
		if (job.id().equals(currentlyExecutedJob.id())) {
			this.taskExecutor.abortTaskExecution();
			if (!server.isTerminated() && server.getActiveCount() > 0) {
				this.createTaskThread.cancel(true);
				this.server.shutdown();
			}
		}
	}

	public boolean isExecutionAborted() {
		return this.taskExecutor.abortedTaskExecution();
	}

	public List<Job> jobs() {
		return jobs;
	}

	// End Execution

}
