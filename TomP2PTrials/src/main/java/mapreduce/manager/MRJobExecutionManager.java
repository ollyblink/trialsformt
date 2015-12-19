package mapreduce.manager;

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
import mapreduce.execution.computation.context.PseudoStoreContext;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.scheduling.ITaskScheduler;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskExecutionScheduler;
import mapreduce.execution.task.taskexecutor.ITaskExecutor;
import mapreduce.execution.task.taskexecutor.ParallelTaskExecutor;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.storage.IDHTConnectionProvider;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	private static final long DEFAULT_SLEEPING_TIME = 100;
	private static final List<Job> DEFAULT_JOB_LIST = Collections.synchronizedList(new ArrayList<>());
	private static final ITaskExecutor DEFAULT_TASK_EXCECUTOR = ParallelTaskExecutor.newInstance();
	private static final ITaskScheduler DEFAULT_TASK_EXECUTION_SCHEDULER = MinAssignedWorkersTaskExecutionScheduler.newInstance();
	private static final IContext DEFAULT_CONTEXT = PseudoStoreContext.newInstance();

	/** Time to live in Milliseconds */
	private static final long DEFAULT_TIME_TO_LIVE = 10000;

	private IDHTConnectionProvider dhtConnectionProvider;
	private IContext context;
	private ITaskScheduler taskExecutionScheduler;
	private ITaskExecutor taskExecutor;
	private List<Job> jobs;
	private MRJobExecutionManagerMessageConsumer messageConsumer;

	private long timeToLive;

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
				.taskExecutionScheduler(DEFAULT_TASK_EXECUTION_SCHEDULER).timeToLive(DEFAULT_TIME_TO_LIVE).context(DEFAULT_CONTEXT);
	}

	public MRJobExecutionManager timeToLive(long timeToLive) {
		this.timeToLive = timeToLive;
		return this;
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
		while (jobs.isEmpty()) {
			try {
				Thread.sleep(DEFAULT_SLEEPING_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		ProcedureInformation procedureInformation = jobs.get(0).procedure(jobs.get(0).currentProcedureIndex());

		// Get the data for the job's current procedure
		this.server = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		this.createTaskThread = server.submit(new Runnable() {

			@Override
			public void run() {
				dhtConnectionProvider.createTasks(jobs.get(0), procedureInformation.tasks(), timeToLive);
			}

		});

		Task task = null;
		List<Task> tasks = procedureInformation.tasks();
		while ((task = this.taskExecutionScheduler.schedule(tasks, timeToLive)) != null && !this.taskExecutor.abortedTaskExecution()) {
			this.dhtConnectionProvider.broadcastExecutingTask(task);
			List<Object> dataForTask = Collections.synchronizedList(new ArrayList<>());
			dhtConnectionProvider.getTaskData(jobs.get(0), task, dataForTask);
			while (this.taskExecutor.abortedTaskExecution()) {
				this.taskExecutor.execute(procedureInformation.procedure(), task.id(), dataForTask, context);// Non-blocking!
			}
			Number160 resultHash = this.context.resultHash();
			this.dhtConnectionProvider.broadcastFinishedTask(task, resultHash);
		}
		if (!this.taskExecutor.abortedTaskExecution()) { // this means that this executor is actually the one that is going to abort the others...

			jobs.get(0).procedure(jobs.get(0).currentProcedureIndex()).isFinished(true);
			this.dhtConnectionProvider.broadcastFinishedAllTasks(jobs.get(0));

			// this.messageConsumer.isBusy(true);

		}

		// awaitMessageConsumerCompletion();
		while (!jobs.get(0).procedure(jobs.get(0).currentProcedureIndex()).isFinished()) {
			System.err.println("Waitng... Waiting...");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		printResults(procedureInformation.tasks());
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

	// End Execution

}
