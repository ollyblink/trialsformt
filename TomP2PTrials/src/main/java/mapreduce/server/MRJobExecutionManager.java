package mapreduce.server;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

import mapreduce.execution.broadcasthandler.messageconsumer.MRJobExecutorMessageConsumer;
import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.computation.context.NullContext;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.execution.scheduling.ITaskScheduler;
import mapreduce.execution.scheduling.RandomTaskScheduler;
import mapreduce.execution.taskresultcomparison.HashTaskResultComparator;
import mapreduce.execution.taskresultcomparison.ITaskResultComparator;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.storage.LocationBean;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	private static final ITaskScheduler DEFAULT_TASK_SCHEDULER = RandomTaskScheduler.newRandomTaskScheduler();
	private static final IContext DEFAULT_CONTEXT = NullContext.newNullContext();
	private static final long DEFAULT_SLEEPING_TIME = 100;
	private static final BlockingQueue<Job> DEFAULT_BLOCKING_QUEUE = new LinkedBlockingQueue<Job>();

	private IDHTConnectionProvider dhtConnectionProvider;
	private ITaskScheduler taskScheduler;
	private IContext context;
	private ITaskResultComparator taskResultComparator;

	private BlockingQueue<Job> jobs;
	private MRJobExecutorMessageConsumer messageConsumer;
	private boolean canExecute;
	private ThreadPoolExecutor server;
	private List<Future<?>> currentThreads = new ArrayList<Future<?>>();
	private boolean abortedTaskExecution;

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.messageConsumer = MRJobExecutorMessageConsumer.newInstance(jobs).jobExecutor(this).canTake(true);
		this.dhtConnectionProvider().broadcastHandler().queue(messageConsumer.queue());
		this.jobs = messageConsumer.jobs();
		new Thread(messageConsumer).start();
	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider, DEFAULT_BLOCKING_QUEUE).taskScheduler(DEFAULT_TASK_SCHEDULER).context(DEFAULT_CONTEXT)
				.taskResultComparator(HashTaskResultComparator.newInstance(dhtConnectionProvider)).canExecute(true);
	}

	// Getter/Setter
	private MRJobExecutionManager dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public MRJobExecutionManager taskScheduler(ITaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
		return this;
	}

	public ITaskScheduler taskScheduler() {
		return this.taskScheduler;
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
		this.abortedTaskExecution = false;
		List<Task> tasks = new LinkedList<Task>(job.tasks(job.currentProcedureIndex()));
		Task task = null;
		while ((task = this.taskScheduler().schedule(tasks)) != null && canExecute()) {
			this.dhtConnectionProvider().broadcastExecutingTask(task);
			this.executeTask(task);
			this.dhtConnectionProvider().broadcastFinishedTask(task);
		}
		if (!this.abortedTaskExecution) { // this means that this executor is actually the one that is going to abort the others...
			this.dhtConnectionProvider().broadcastFinishedAllTasks(job);
			// abortTaskExecution();
			logger.info("This executor aborts task execution!");
			messageConsumer.removeRemainingMessagesForThisTask(job.id());

			// Only printing
			BlockingQueue<Task> ts = job.tasks(job.currentProcedureIndex());
			for (Task t : ts) {
				logger.info("Task " + t.id());
				for (PeerAddress pAddress : t.allAssignedPeers()) {
					logger.info(pAddress.inetAddress() + ":" + pAddress.tcpPort() + ": " + t.statiForPeer(pAddress));
				}
			}
		}
		while (this.messageConsumer.isBusy()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// Task Comparison & new task creation
		tasks = new LinkedList<Task>(job.tasks(job.currentProcedureIndex())); // Eventually can also reuse it directly from above?

		while ((task = this.taskScheduler().schedule(tasks)) != null && canExecute()) {
			this.dhtConnectionProvider().broadcastExecutingCompareTaskResults(task);
			Tuple<PeerAddress, Integer> taskResultEvaluationResult = this.taskResultComparator.evaluateTaskResults(task);
			task.dataLocationHashPeerAddress(taskResultEvaluationResult.first()).dataLocationHashJobStatusIndex(taskResultEvaluationResult.second());
			this.dhtConnectionProvider().broadcastFinishedCompareTaskResults(task);
		}
		// if (!this.abortedTaskExecution) { // this means that this executor is actually the one that is going to abort the others...
		// this.dhtConnectionProvider().broadcastFinishedAllTasks(job);
		// // abortTaskExecution();
		// logger.info("This executor aborts task execution!");
		// messageConsumer.removeRemainingMessagesForThisTask(job.id());
		//
		// // Only printing
		// BlockingQueue<Task> ts = job.tasks(job.currentProcedureIndex());
		// for (Task t : ts) {
		// logger.info("Task " + t.id());
		// for (PeerAddress pAddress : t.allAssignedPeers()) {
		// logger.info(pAddress.inetAddress() + ":" + pAddress.tcpPort() + ": " + t.statiForPeer(pAddress));
		// }
		// }
		// }

	}

	private void executeTask(final Task task) {
		this.context().task(task);
		int nThreads = Runtime.getRuntime().availableProcessors();
		this.server = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		final Multimap<Object, Object> dataForTask = dhtConnectionProvider().getTaskData(task,
				LocationBean.newInstance(this.dhtConnectionProvider().peerAddress(), task.dataLocationHashJobStatusIndex(), task.procedure()));
		for (final Object key : dataForTask.keySet()) {
			Runnable run = new Runnable() {

				@Override
				public void run() {
					callProcedure(key, dataForTask.get(key), task.procedure());
				}
			};
			Future<?> submit = server.submit(run);
			this.currentThreads.add(submit);
		}
		server.shutdown();
		while (!server.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	private void callProcedure(Object key, Collection<Object> values, IMapReduceProcedure procedure) {

		try {
			Method process = procedure.getClass().getMethods()[0];
			process.invoke(procedure, new Object[] { key, values, this.context() });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void abortTaskExecution() {
		this.abortedTaskExecution = true;
		System.err.println("Aborting task");
		if (!server.isTerminated() && server.getActiveCount() > 0) {
			for (Future<?> run : this.currentThreads) {
				run.cancel(true);
			}
			server.shutdown();
			while (!server.isTerminated()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		System.err.println("Task aborted");
	}

	// End Execution
}
