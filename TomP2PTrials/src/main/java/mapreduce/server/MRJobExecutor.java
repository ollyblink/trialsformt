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
import mapreduce.execution.scheduling.MinAssignedWorkersTaskScheduler;
import mapreduce.execution.scheduling.RandomTaskScheduler;
import mapreduce.storage.IDHTConnectionProvider;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutor {
	private static final ITaskScheduler DEFAULT_TASK_SCHEDULER = RandomTaskScheduler.newRandomTaskScheduler();
	private static final IContext DEFAULT_CONTEXT = NullContext.newNullContext();
	private static final long DEFAULT_SLEEPING_TIME = 100;

	private static Logger logger = LoggerFactory.getLogger(MRJobExecutor.class);

	private IDHTConnectionProvider dhtConnectionProvider;
	private ITaskScheduler taskScheduler;

	private IContext context;

	private BlockingQueue<Job> jobs;
	private MRJobExecutorMessageConsumer messageConsumer;
	private boolean canExecute;
	private ThreadPoolExecutor server;
	private List<Future<?>> currentThreads = new ArrayList<Future<?>>();
	private boolean abortedTaskExecution;

	private MRJobExecutor(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.messageConsumer = MRJobExecutorMessageConsumer.newMRJobExecutorMessageConsumer(jobs).jobExecutor(this).canTake(true);
		this.dhtConnectionProvider().broadcastHandler().queue(messageConsumer.queue());
		this.jobs = messageConsumer.jobs();
		new Thread(messageConsumer).start();
	}

	public static MRJobExecutor newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutor(dhtConnectionProvider, new LinkedBlockingQueue<Job>()).taskScheduler(DEFAULT_TASK_SCHEDULER).canExecute(true);
	}

	// Getter/Setter
	private MRJobExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public MRJobExecutor taskScheduler(ITaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
		return this;
	}

	public ITaskScheduler taskScheduler() {
		if (this.taskScheduler == null) {
			this.taskScheduler = DEFAULT_TASK_SCHEDULER;
		}
		return this.taskScheduler;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public MRJobExecutor context(IContext context) {
		this.context = context;
		return this;
	}

	public IContext context() {
		if (context == null) {
			this.context = DEFAULT_CONTEXT;
		}
		return this.context;
	}

	public MRJobExecutor canExecute(boolean canExecute) {
		this.canExecute = canExecute;
		return this;
	}

	public boolean canExecute() {
		return this.canExecute;
	}

	public Job getJob() {
		return jobs.peek();
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
		this.abortedTaskExecution = false;
		List<Task> tasks = new LinkedList<Task>(job.tasks(job.currentProcedureIndex()));
		Task task = null;
		while ((task = this.taskScheduler().schedule(tasks)) != null && canExecute()) {
			this.dhtConnectionProvider().broadcastExecutingTask(task);
			this.executeTask(task);
			this.dhtConnectionProvider().broadcastFinishedTask(task);
		}
		if (!canExecute()) {
			System.err.println("Cannot execute! use MRJobSubmitter::canExecute(true) to enable execution");
		}
		if (!this.abortedTaskExecution) { // this means that this executor is actually the one that is going to abort the others...
			this.dhtConnectionProvider().broadcastFinishedAllTasks(job);
			// abortTaskExecution();
			System.err.println("Aborted Tasks!!");
			messageConsumer.removeRemainingMessagesForThisTask(job.id());
			BlockingQueue<Task> ts = job.tasks(job.currentProcedureIndex());
			for (Task t : ts) {
				System.err.println("Task " + t.id());
				for (PeerAddress pAddress : t.allAssignedPeers()) {
					System.err.println(pAddress.inetAddress() + ":" + pAddress.tcpPort() + ": " + t.statiForPeer(pAddress));
				}
			}
		}
		// all tasks finished, broadcast result
		// jobs.poll();
		// startExecuting();

	}

	private void executeTask(final Task task) {
		this.context().task(task);
		int nThreads = Runtime.getRuntime().availableProcessors();
		this.server = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		final Multimap<Object, Object> dataForTask = dhtConnectionProvider().getDataForTask(task);
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
