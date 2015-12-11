package mapreduce.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

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
import mapreduce.storage.dhtmaintenance.IDHTDataCleaner;
import mapreduce.storage.dhtmaintenance.ParallelDHTDataCleaner;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	private static final long DEFAULT_SLEEPING_TIME = 100;
	private static final CopyOnWriteArrayList<Job> DEFAULT_BLOCKING_QUEUE = new CopyOnWriteArrayList<Job>();
	private static final ITaskExecutor DEFAULT_TASK_EXCECUTOR = ParallelTaskExecutor.newInstance();
	private static final ITaskScheduler DEFAULT_TASK_EXECUTION_SCHEDULER = MinAssignedWorkersTaskExecutionScheduler.newInstance();
	private static final IContext DEFAULT_CONTEXT = PseudoStoreContext.newInstance();

	private IDHTConnectionProvider dhtConnectionProvider;
	private IContext context;
	private ITaskScheduler taskExecutionScheduler;
	private ITaskExecutor taskExecutor;

	private CopyOnWriteArrayList<Job> jobs;
	private MRJobExecutionManagerMessageConsumer messageConsumer;
	private boolean canExecute;

	private IDHTDataCleaner dhtDataCleaner;

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider, CopyOnWriteArrayList<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.messageConsumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(this).canTake(true);
		this.dhtConnectionProvider().broadcastHandler().queue(messageConsumer.queue());
		this.jobs = jobs;
		new Thread(messageConsumer).start();
	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider, DEFAULT_BLOCKING_QUEUE).taskExecutor(DEFAULT_TASK_EXCECUTOR)
				.dhtDataCleaner(ParallelDHTDataCleaner.newInstance(dhtConnectionProvider.bootstrapIP(), dhtConnectionProvider.bootstrapPort()))
				.taskExecutionScheduler(DEFAULT_TASK_EXECUTION_SCHEDULER).context(DEFAULT_CONTEXT).canExecute(true);
	}

	public MRJobExecutionManager dhtDataCleaner(IDHTDataCleaner dhtDataCleaner) {
		this.dhtDataCleaner = dhtDataCleaner;
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

	public MRJobExecutionManager canExecute(boolean canExecute) {
		this.canExecute = canExecute;
		return this;
	}

	public boolean canExecute() {
		return this.canExecute;
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
		if (!canExecute()) {
			System.err.println("Cannot execute! use MRJobSubmitter::canExecute(true) to enable execution");
		}
		List<Task> tasks = new ArrayList<Task>(jobs.get(0).tasks(jobs.get(0).currentProcedureIndex()));
		Task task = null;
		while ((task = this.taskExecutionScheduler.schedule(tasks)) != null && !this.taskExecutor.abortedTaskExecution() && canExecute()) {
			this.dhtConnectionProvider.broadcastExecutingTask(task);
			ArrayListMultimap<Object, Object> tmp = ArrayListMultimap.create();
			Multimap<Object, Object> dataForTask = Multimaps.synchronizedListMultimap(tmp);
			dhtConnectionProvider.getTaskData(task, task.initialDataLocation(), dataForTask);
			while (this.taskExecutor.abortedTaskExecution()) {
				this.taskExecutor.executeTask(task, context, dataForTask);// Non-blocking!
			}
			Number160 resultHash = this.context.resultHash();
			this.dhtConnectionProvider.broadcastFinishedTask(task, resultHash);
		}
		if (!this.taskExecutor.abortedTaskExecution()) { // this means that this executor is actually the one that is going to abort the others...

			this.dhtConnectionProvider.broadcastFinishedAllTasks(jobs.get(0));

			// this.messageConsumer.isBusy(true);

		}

		// awaitMessageConsumerCompletion();
		while (!jobs.get(0).isFinishedFor(jobs.get(0).procedure(jobs.get(0).currentProcedureIndex()))) {
			System.err.println("Waitng... Waiting...");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		printResults(jobs.get(0));
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

	private void cleanUpDHT(final Multimap<Task, Tuple<PeerAddress, Integer>> dataToRemove) {
		new Thread(new Runnable() {

			@Override
			public void run() {
				dhtDataCleaner.removeDataFromDHT(dataToRemove);
			}

		}).start();
	}

	public void abortExecution(Job job) {
		if (job.id().equals(jobs.get(0).id())) {
			this.taskExecutor.abortTaskExecution();
		}
	}

	private void printResults(Job job) {
		// Only printing
		BlockingQueue<Task> ts = job.tasks(job.currentProcedureIndex());
		logger.info("All final data locations ");
		for (Task t : ts) {
			PeerAddress p = t.finalDataLocation().first();
			logger.info("<" + p.tcpPort() + ", " + t.finalDataLocation().second() + ">");
		}

	}

	public boolean isExecutionAborted() {
		return this.taskExecutor.abortedTaskExecution();
	}

	// End Execution

}
