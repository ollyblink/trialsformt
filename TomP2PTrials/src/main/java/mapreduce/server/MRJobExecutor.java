package mapreduce.server;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.MessageConsumer;
import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.computation.context.NullContext;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.KeyValuePair;
import mapreduce.execution.jobtask.Task;
import mapreduce.execution.scheduling.ITaskScheduler;
import mapreduce.execution.scheduling.MinAssignedWorkersTaskScheduler;
import mapreduce.storage.IDHTConnectionProvider;

public class MRJobExecutor {
	private static final ITaskScheduler DEFAULT_TASK_SCHEDULER = MinAssignedWorkersTaskScheduler.newRandomTaskScheduler();
	private static final IContext DEFAULT_CONTEXT = NullContext.newNullContext();
	private static final long DEFAULT_SLEEPING_TIME = 100;

	private static Logger logger = LoggerFactory.getLogger(MRJobExecutor.class);

	private IDHTConnectionProvider dhtConnectionProvider;
	private ITaskScheduler taskScheduler;

	private IContext context;

	private BlockingQueue<Job> jobs;

	private MRJobExecutor(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		MessageConsumer messageConsumer = MessageConsumer.newMessageConsumer(jobs).canTake(true);
		new Thread(messageConsumer).start();
		dhtConnectionProvider.broadcastHandler().queue(messageConsumer.queue());
		this.jobs = messageConsumer.jobs();

	}

	public static MRJobExecutor newJobExecutor(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		return new MRJobExecutor(dhtConnectionProvider, jobs);
	}

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

	public void executeJob(Job job) {
		logger.warn("executing job: " + job.id());
		BlockingQueue<Task> tasks = job.tasksFor(job.nextProcedure());

		executeTasksForJob(job, tasks);

	}

	private void executeTasksForJob(Job job, BlockingQueue<Task> tasks) {
		int maxNrOfFinishedPeers = job.maxNrOfFinishedPeers();
		Task task = this.taskScheduler().schedule(new LinkedList<Task>(tasks));
 		if (task.totalNumberOfFinishedExecutions() < maxNrOfFinishedPeers) {
 			this.dhtConnectionProvider().broadcastTaskSchedule(task);
			executeTask(task);
			this.dhtConnectionProvider().broadcastFinishedTask(task);
			this.executeTasksForJob(job, tasks); 
		} else {// check if all tasks finished 
			boolean allHaveFinished = allTasksHaveFinished(maxNrOfFinishedPeers, tasks);
			if (allHaveFinished) {
				this.dhtConnectionProvider.broadcastFinishedAllTasks(job);
			} else {
				this.executeTasksForJob(job, tasks);
			}
		}
	}

	private boolean allTasksHaveFinished(int maxNrOfFinishedPeers, BlockingQueue<Task> tasks) {
		boolean allHaveFinished = true;
		for (Task t : tasks) {
			if (t.totalNumberOfFinishedExecutions() < maxNrOfFinishedPeers) {
				allHaveFinished = false;
			}
		}
		return allHaveFinished;
	}

	private void executeTask(final Task task) {
		ExecutorService server = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		final List<KeyValuePair<Object, Object>> dataForTask = dhtConnectionProvider().getDataForTask(task);
		for (final Object key : task.keys()) {
			server.execute(new Runnable() {

				@Override
				public void run() {
					for (KeyValuePair<Object, Object> kvPair : dataForTask) {
						callProcedure(key, kvPair.value(), task.procedure());
					}
				}
			});
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

	private void callProcedure(Object key, Object value, IMapReduceProcedure<?, ?, ?, ?> procedure) {
		Method process = procedure.getClass().getMethods()[0];
		try {
			process.invoke(procedure, new Object[] { key, value, context() });
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	public void start(boolean useDiskStorage) {
		logger.info("Try to connect.");
		dhtConnectionProvider.connect();
		startExecuting();
	}

	private void startExecuting() {
		while (jobs.isEmpty()) {
			try {
				// logger.warn("Job size: "+jobs.size());
				Thread.sleep(DEFAULT_SLEEPING_TIME);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		executeJob(jobs.peek());
	}

	public void shutdown() {
		dhtConnectionProvider.shutdown();
	}

	public Job getJob() {
		return jobs.peek();
	}

}
