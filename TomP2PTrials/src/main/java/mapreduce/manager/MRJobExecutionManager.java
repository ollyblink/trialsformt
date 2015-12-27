package mapreduce.manager;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;

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
import mapreduce.execution.computation.context.DHTStorageContext;
import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.scheduling.ITaskScheduler;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	// private static final ITaskExecutor DEFAULT_TASK_EXCECUTOR = ParallelTaskExecutor.newInstance();

	private IDHTConnectionProvider dhtConnectionProvider;
	// private IContext context;
	// private ITaskExecutor taskExecutor;
	private List<Job> jobs;
	private MRJobExecutionManagerMessageConsumer messageConsumer;

	// private Future<?> createTaskThread;

	private ThreadPoolExecutor server;

	private String id;

	private Job currentlyExecutedJob;

	private List<Future<?>> activeThreads = SyncedCollectionProvider.syncedArrayList();

	private ITaskScheduler taskExecutionScheduler;

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider, List<Job> jobs) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.dhtConnectionProvider = dhtConnectionProvider.owner(this.id);
		this.messageConsumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(this).canTake(true);
		this.dhtConnectionProvider.addMessageQueueToBroadcastHandlers(messageConsumer.queue());
		this.taskExecutionScheduler = MinAssignedWorkersTaskExecutionScheduler.newInstance();
		this.jobs = jobs;

	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider, Collections.synchronizedList(new ArrayList<>()));
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

	public void start() {
//		dhtConnectionProvider.connect();
		Thread messageConsumerThread = new Thread(messageConsumer);
		messageConsumerThread.start();
	}

	// End Maintenance
	// Execution

	public void execute(Job job) {
		this.currentlyExecutedJob = job;
		ProcedureInformation procedureInformation = currentlyExecutedJob.currentProcedure();
		taskExecutionScheduler.procedureInformation(procedureInformation);
		logger.info("Got job: " + currentlyExecutedJob.id() + ", starting procedure " + procedureInformation);

		// Get the data for the job's current procedure
		this.server = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		String jobProcedureDomain = procedureInformation.jobProcedureDomain();
		// List<FutureGet> futureGetTaskExecutorDomains = syncedArrayList();

		// Get all procedure keys!! Create all the tasks for each key!!!
		logger.info("run()");
		logger.info(DomainProvider.PROCEDURE_KEYS);
		logger.info(jobProcedureDomain);

		List<Task> tasks = procedureInformation.tasks();
		dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_KEYS, jobProcedureDomain).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				System.err.println("Job Proc domain: " + jobProcedureDomain);
				if (future.isSuccess()) {
					try {
						if (future.dataMap() != null) {
							for (Number640 n : future.dataMap().keySet()) {
								Object key = future.dataMap().get(n).object();
								Task task = Task.newInstance(key, job.id());
								if (tasks.contains(task)) {// Don't need to add it more, got it e.g. from a BC
									System.err.println("tasks.contains(task)");
									return;
								} else {
									System.err.println("KEY: " + key.toString());
									dhtConnectionProvider.getAll(key.toString(), jobProcedureDomain).addListener(new BaseFutureAdapter<FutureGet>() {

										@Override
										public void operationComplete(FutureGet future) throws Exception {
											if (future.isSuccess()) {
												try {
													if (future.dataMap() != null) {
														for (Number640 n : future.dataMap().keySet()) {
															Object taskExecutorDomain = future.dataMap().get(n).object();

															System.err.println("taskExecutorDomain: " + taskExecutorDomain);
															task.finalDataLocationDomains(taskExecutorDomain.toString());
														}
														if (!tasks.contains(task)) {
															System.err.println("!tasks.contains(task)");
															tasks.add(task);
															executeTask(taskExecutionScheduler.schedule(tasks));
														}
													}
												} catch (IOException e) {
													System.err.println("failed");
													logger.info("failed");
													// dhtConnectionProvider.broadcastFailedTask(taskToDistribute);
												}
											} else {
												// dhtConnectionProvider.broadcastFailedJob(jobs.get(0));
												System.err.println("failed");
												logger.info("failed");
											}
										}

									});
								}
							}
						}
					} catch (IOException e) {
						// dhtConnectionProvider.broadcastFailedTask(taskToDistribute);
						System.err.println("failed");
						logger.info("failed");
					}
				} else {
					// dhtConnectionProvider.broadcastFailedJob(jobs.get(0));
					System.err.println("failed");
					logger.info("failed");
				}
			}

		});

	}

	public void executeTask(Task task) {
		if (task != null && !task.isActive()) {
			System.err.println("Task to execute: " + task);
			List<String> finalDataLocations = task.finalDataLocationDomains();
			List<Object> valuesCollector = syncedArrayList();
			List<FutureGet> futureGetData = syncedArrayList();

			dhtConnectionProvider.broadcastExecutingTask(task);
			// TODO build in that the data retrieval may take a certain number of repetitions when failed before being broadcasted as failed
			for (int i = 0; i < finalDataLocations.size(); ++i) {
				// Now we actually wanna retrieve the data from the specified locations...
				String taskExecutorDomainCombination = currentlyExecutedJob.currentProcedureDomain() + "_" + finalDataLocations.get(i);
				futureGetData
						.add(dhtConnectionProvider.getAll(task.id(), taskExecutorDomainCombination).addListener(new BaseFutureAdapter<FutureGet>() {

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

						}));
			}
			Futures.whenAllSuccess(futureGetData).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
				@Override
				public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
					IContext context = DHTStorageContext.create().task(task).dhtConnectionProvider(dhtConnectionProvider)
							.subsequentJobProcedureDomain(currentlyExecutedJob.subsequentJobProcedureDomain());
					// taskExecutor.execute(job.currentProcedure().procedure(), task.id(), valuesCollector, context);

					context.task().isActive(true);
					currentlyExecutedJob.currentProcedure().procedure().process(task.id(), valuesCollector, context);
					context.broadcastResultHash();
					context.task().isActive(false);
					Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
						@Override
						public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
							Task nextTask = taskExecutionScheduler.schedule(currentlyExecutedJob.currentProcedure().tasks());
							if (currentlyExecutedJob.currentProcedure().isFinished()) {
								// May have been aborted from outside and thus, hasn't finished yet (in case abortExecution(job) was called
								dhtConnectionProvider.broadcastFinishedAllTasksOfProcedure(currentlyExecutedJob);
								logger.info("Broadcast finished Procedure");
							} else {
								logger.info("executing next task");
								logger.info("tasks: "+currentlyExecutedJob.currentProcedure().tasks());
								executeTask(nextTask);
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

	public void abortExecution(Job job) {
		if (job.id().equals(currentlyExecutedJob.id())) {
			logger.info("Aborting task");
			if (!server.isTerminated() && server.getActiveCount() > 0) {
				for (Future<?> run : this.activeThreads) {
					run.cancel(true);
				}
				cleanUp();
			}

			logger.info("Task aborted");
		}
	}

	private void cleanUp() {
		server.shutdown();
		while (!server.isTerminated()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.server = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
	}

	public boolean isExecutionAborted() {
		// return this.taskExecutor.abortedTaskExecution();
		return false;
	}

	public List<Job> jobs() {
		return jobs;
	}

	// End Execution

}
