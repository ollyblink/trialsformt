package mapreduce.manager;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.computation.context.NullContext;
import mapreduce.execution.executor.ITaskExecutor;
import mapreduce.execution.executor.ParallelTaskExecutor;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.execution.scheduling.ITaskScheduler;
import mapreduce.execution.scheduling.taskexecutionscheduling.RandomTaskExecutionScheduler;
import mapreduce.execution.taskresultcomparison.HashTaskResultComparator;
import mapreduce.execution.taskresultcomparison.ITaskResultComparator;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobExecutorMessageConsumer;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCStatusType;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.storage.LocationBean;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	private static final long DEFAULT_SLEEPING_TIME = 100;
	private static final BlockingQueue<Job> DEFAULT_BLOCKING_QUEUE = new LinkedBlockingQueue<Job>();
	private static final ITaskExecutor DEFAULT_TASK_EXCECUTOR = ParallelTaskExecutor.newInstance();
	private static final ITaskScheduler DEFAULT_TASK_EXECUTION_SCHEDULER = RandomTaskExecutionScheduler.newInstance();
	private static final IContext DEFAULT_CONTEXT = NullContext.newNullContext();
	private static final ITaskResultComparator DEFAULT_TASK_RESULT_COMPARATOR = HashTaskResultComparator.newInstance();

	private IDHTConnectionProvider dhtConnectionProvider;
	private IContext context;
	private ITaskScheduler taskExecutionScheduler;
	private ITaskExecutor taskExecutor;
	private ITaskScheduler taskResultComparisonScheduler;
	private ITaskResultComparator taskResultComparator;

	private BlockingQueue<Job> jobs;
	private MRJobExecutorMessageConsumer messageConsumer;
	private boolean canExecute;

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.messageConsumer = MRJobExecutorMessageConsumer.newInstance(jobs).jobExecutor(this).canTake(true);
		this.dhtConnectionProvider().broadcastHandler().queue(messageConsumer.queue());
		this.jobs = messageConsumer.jobs();
		new Thread(messageConsumer).start();
	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider, DEFAULT_BLOCKING_QUEUE).taskExecutor(DEFAULT_TASK_EXCECUTOR)
				.taskExecutionScheduler(DEFAULT_TASK_EXECUTION_SCHEDULER).context(DEFAULT_CONTEXT)
				.taskResultComparator(DEFAULT_TASK_RESULT_COMPARATOR).canExecute(true);
	}

	public MRJobExecutionManager taskExecutor(ITaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
		return this;
	}

	public ITaskExecutor taskExecutor() {
		return this.taskExecutor;
	}

	// Getter/Setter
	private MRJobExecutionManager dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public MRJobExecutionManager taskExecutionScheduler(ITaskScheduler taskExecutionScheduler) {
		this.taskExecutionScheduler = taskExecutionScheduler;
		return this;
	}

	public ITaskScheduler taskExecutionScheduler() {
		return this.taskExecutionScheduler;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public MRJobExecutionManager context(IContext context) {
		this.context = context;
		return this;
	}

	public IContext context() {
		return this.context;
	}

	public MRJobExecutionManager canExecute(boolean canExecute) {
		this.canExecute = canExecute;
		return this;
	}

	public boolean canExecute() {
		return this.canExecute;
	}

	public Job getJob() {
		return jobs.peek();
	}

	public ITaskResultComparator taskResultComparator() {
		return this.taskResultComparator;
	}

	public MRJobExecutionManager taskResultComparator(ITaskResultComparator taskResultComparator) {
		this.taskResultComparator = taskResultComparator;
		return this;
	}
	// END GETTER/SETTER

	// Maintenance
	public void start() {
		logger.info("Try to connect.");
		dhtConnectionProvider.connect();
		startExecuting();
	}

	public void shutdown() {
		dhtConnectionProvider.shutdown();
	}
	// End Maintenance

	// Execution
	private void startExecuting() {
		while (jobs.isEmpty()) {
			try {
				Thread.sleep(DEFAULT_SLEEPING_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		executeJob(jobs.peek());
	}

	private void executeJob(Job job) {
		if (!canExecute()) {
			System.err.println("Cannot execute! use MRJobSubmitter::canExecute(true) to enable execution");
		}
		this.taskExecutor.abortedTaskExecution(false);
		List<Task> tasks = new LinkedList<Task>(job.tasks(job.currentProcedureIndex()));
		Task task = null;
		while ((task = this.taskExecutionScheduler.schedule(tasks)) != null && canExecute()) {
			this.dhtConnectionProvider.broadcastExecutingTask(task);
			final Multimap<Object, Object> dataForTask = dhtConnectionProvider.getTaskData(task,
					LocationBean.newInstance(task.initialDataLocation(), task.procedure()));

			this.taskExecutor.executeTask(task, context, dataForTask);
			this.dhtConnectionProvider.broadcastFinishedTask(task);
		}
		if (!this.taskExecutor.abortedTaskExecution()) { // this means that this executor is actually the one that is going to abort the others...
			// Clean up all "executing" tasks
			cleanUp(job);
			this.dhtConnectionProvider.broadcastFinishedAllTasks(job);
			// abortTaskExecution();
			logger.info("This executor aborts task execution!");
			this.messageConsumer.removeMessagesFromJobWithStati(job.id());

			// Only printing
			// BlockingQueue<Task> ts = job.tasks(job.currentProcedureIndex());
			// for (Task t : ts) {
			// logger.info("Task " + t.id());
			// for (PeerAddress pAddress : t.allAssignedPeers()) {
			// logger.info(pAddress.inetAddress() + ":" + pAddress.tcpPort() + ": " + t.statiForPeer(pAddress));
			// }
			// }
		}
		while (this.messageConsumer.isBusy()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// Task Comparison & new task creation
		tasks = new LinkedList<Task>(job.tasks(job.currentProcedureIndex())); // Hypothetically, can also reuse it directly from above?

		task = null;
		while ((task = this.taskResultComparisonScheduler.schedule(tasks)) != null && canExecute()) {
			this.dhtConnectionProvider.broadcastExecutingCompareTaskResults(task);
			Tuple<PeerAddress, Integer> taskResultEvaluationResult = this.taskResultComparator.evaluateTaskResults(task);
			task.finalDataLocation(taskResultEvaluationResult);
			this.dhtConnectionProvider.broadcastFinishedCompareTaskResults(task);
		}
		if (!this.taskResultComparator.abortedTaskComparisons()) { // this means that this executor is actually the one that is going to abort the
																	// others...
			// Clean up all "executing" tasks
			cleanUp(job);
			this.dhtConnectionProvider.broadcastFinishedAllTaskComparisons(job);
			// abortTaskExecution();
			logger.info("This executor aborts task comparisons!");
			this.messageConsumer.removeMessagesFromJobWithStati(job.id());

			// Only printing
			// BlockingQueue<Task> ts = job.tasks(job.currentProcedureIndex());
			// for (Task t : ts) {
			// logger.info("Task " + t.id());
			// for (PeerAddress pAddress : t.allAssignedPeers()) {
			// logger.info(pAddress.inetAddress() + ":" + pAddress.tcpPort() + ": " + t.statiForPeer(pAddress));
			// }
			// }
		}

		while (this.messageConsumer.isBusy()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// Create new tasks
		// submit new tasks

	}

	private void cleanUp(final Job job) {
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		for (final Task task : tasks) {
			for (final PeerAddress peerAddress : task.allAssignedPeers()) {
				final int jobStatusIndex = task.statiForPeer(peerAddress).indexOf(BCStatusType.EXECUTING_TASK); // there is at most one
																												// "EXECUTING_TASK" per PeerAddress
				task.statiForPeer(peerAddress).remove(jobStatusIndex);
				new Thread(new Runnable() {

					@Override
					public void run() {
						dhtConnectionProvider.removeTaskResultsFor(task,
								LocationBean.newInstance(Tuple.newInstance(peerAddress, jobStatusIndex), task.procedure()));
					}

				}).start();

			}
		}
	}

	public void abortTaskExecution() {
		this.taskExecutor.abortTaskExecution();
	}

	public void abortTaskComparison() {
		this.taskResultComparator.abortTaskComparison();
	}

	// End Execution
}
