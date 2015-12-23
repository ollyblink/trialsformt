package mapreduce.manager;

import java.io.IOError;
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
import mapreduce.manager.conditions.EmptyListCondition;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.TimeToLive;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;

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

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider, List<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.messageConsumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(this).canTake(true);
		this.dhtConnectionProvider().broadcastHandler().queue(messageConsumer.queue());

		this.jobs = jobs;
		new Thread(messageConsumer).start();
	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider, DEFAULT_JOB_LIST).taskExecutor(DEFAULT_TASK_EXCECUTOR)
				.taskExecutionScheduler(DEFAULT_TASK_EXECUTION_SCHEDULER).context(DEFAULT_CONTEXT);
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

		executeJob();
	}

	// End Maintenance
	// Execution

	private void executeJob() {
		if (TimeToLive.INSTANCE.cancelOnTimeout(jobs, EmptyListCondition.create())) {

			ProcedureInformation procedureInformation = jobs.get(0).procedure(jobs.get(0).currentProcedureIndex());

			// Get the data for the job's current procedure
			this.server = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

			List<FutureGet> futureTaskExecutorDomains = Collections.synchronizedList(new ArrayList<>());
			List<Task> tasks = procedureInformation.tasks();
			this.createTaskThread = server.submit(new Runnable() {

				@Override
				public void run() {
					dhtConnectionProvider.createTasks(jobs.get(0), futureTaskExecutorDomains, tasks);
				}

			});
			Futures.whenAnySuccess(futureTaskExecutorDomains).addListener(new BaseFutureListener<FutureDone<FutureGet>>() {

				@Override
				public void operationComplete(FutureDone<FutureGet> future) throws Exception {
					if (future.isSuccess()) {
						Task task = null;
						while ((task = taskExecutionScheduler.schedule(tasks)) != null && !taskExecutor.abortedTaskExecution()) {
							final Task taskToDistribute = task;
							dhtConnectionProvider.broadcastExecutingTask(task);

							// Now we actually wanna retrieve the data from the specified locations...
							String keyString = task.id().toString();
							String domainString = DomainProvider.INSTANCE.jobProcedureDomain(jobs.get(0)) + "_"
									+ DomainProvider.INSTANCE.executorTaskDomain(task.id(), task.finalDataLocation().first().peerId().toString(),
											task.finalDataLocation().second());

							dhtConnectionProvider.getAll(keyString, domainString).addListener(new BaseFutureListener<FutureGet>() {

								@Override
								public void operationComplete(FutureGet future) throws Exception {
									if (future.isSuccess()) {
										List<Object> dataForTask = Collections.synchronizedList(new ArrayList<>());
										try {
											if (future.dataMap() != null) {
												for (Number640 n : future.dataMap().keySet()) {
													Object value = future.dataMap().get(n).object();
													dataForTask.add(value);
												}
												taskExecutor.execute(procedureInformation.procedure(), keyString, dataForTask, context);// Non-blocking!
												dhtConnectionProvider.broadcastFinishedTask(taskToDistribute, context.resultHash());
											}
										} catch (IOException e) {
											dhtConnectionProvider.broadcastFailedTask(taskToDistribute);
										}
									}
								}

								@Override
								public void exceptionCaught(Throwable t) throws Exception {
									dhtConnectionProvider.broadcastFailedTask(taskToDistribute);
								}

							});

						}
						if (!taskExecutor.abortedTaskExecution()) { // this means that this executor is actually the one that is going to abort the
																	// others...

							procedureInformation.isFinished(true);
							dhtConnectionProvider.broadcastFinishedAllTasks(jobs.get(0));

						}
					} else {
						logger.warn("Something wrong");
					}
				}

				@Override
				public void exceptionCaught(Throwable t) throws Exception {
					// TODO Auto-generated method stub

				}

			});

			// awaitMessageConsumerCompletion();
			// while (!procedureInformation.isFinished()) {
			// System.err.println("Waitng... Waiting...");
			// try {
			// Thread.sleep(100);
			// } catch (InterruptedException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// }
			// printResults(procedureInformation.tasks());
			// Create new tasks

			// if (jobs.get(0).hasNextProcedure()) {
			// // Create new tasks
			// // add new tasks to job
			//
			// } else {
			// jobs.remove(0);
			// }

			// Clean up data from old tasks
			// cleanUpDHT(jobs.get(0).taskDataToRemove(jobs.get(0).currentProcedureIndex()));
			// logger.info("clean up");
			executeJob();
		} else {
			handleTimeout();
		}

	}

	private void handleTimeout() {
		// TODO Auto-generated method stub

	}

	public void abortExecution(Job job) {
		if (job.id().equals(jobs.get(0).id())) {
			this.taskExecutor.abortTaskExecution();
			if (!server.isTerminated() && server.getActiveCount() > 0) {
				this.createTaskThread.cancel(true);
				this.server.shutdown();
			}
		}
	}

	private void printResults(List<Task> tasks) {
		logger.info("All final data locations ");
		for (Task t : tasks) {
			PeerAddress p = null;
			synchronized (tasks) {
				p = t.finalDataLocation().first();
			}
			if (p != null) {
				logger.info("<" + p.tcpPort() + ", " + t.finalDataLocation().second() + ">");
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
