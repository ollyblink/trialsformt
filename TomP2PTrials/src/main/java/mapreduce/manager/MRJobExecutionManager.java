package mapreduce.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

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
import mapreduce.storage.LocationBean;
import net.tomp2p.peers.Number160;

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	private static final long DEFAULT_SLEEPING_TIME = 100;
	private static final BlockingQueue<Job> DEFAULT_BLOCKING_QUEUE = new LinkedBlockingQueue<Job>();
	private static final ITaskExecutor DEFAULT_TASK_EXCECUTOR = ParallelTaskExecutor.newInstance();
	private static final ITaskScheduler DEFAULT_TASK_EXECUTION_SCHEDULER = MinAssignedWorkersTaskExecutionScheduler.newInstance();
	private static final IContext DEFAULT_CONTEXT = PseudoStoreContext.newInstance(); 

	private IDHTConnectionProvider dhtConnectionProvider;
	private IContext context;
	private ITaskScheduler taskExecutionScheduler;
	private ITaskExecutor taskExecutor;

	private BlockingQueue<Job> jobs;
	private MRJobExecutionManagerMessageConsumer messageConsumer;
	private boolean canExecute;

	private MRJobExecutionManager(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.messageConsumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(this).canTake(true);
		this.dhtConnectionProvider().broadcastHandler().queue(messageConsumer.queue());
		this.jobs = messageConsumer.jobs();
		new Thread(messageConsumer).start();
	}

	public static MRJobExecutionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutionManager(dhtConnectionProvider, DEFAULT_BLOCKING_QUEUE).taskExecutor(DEFAULT_TASK_EXCECUTOR)
				.taskExecutionScheduler(DEFAULT_TASK_EXECUTION_SCHEDULER).context(DEFAULT_CONTEXT).canExecute(true);
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
		List<Task> tasks = new ArrayList<Task>(job.tasks(job.currentProcedureIndex()));
		Task task = null;
		while ((task = this.taskExecutionScheduler.schedule(tasks)) != null && !this.taskExecutor.abortedTaskExecution() && canExecute()) {
			this.dhtConnectionProvider.broadcastExecutingTask(task);
			final Multimap<Object, Object> dataForTask = dhtConnectionProvider.getTaskData(task, task.initialDataLocation());

			this.taskExecutor.executeTask(task, context, dataForTask);// Non-blocking!
			Number160 resultHash = this.context.resultHash();
			this.dhtConnectionProvider.broadcastFinishedTask(task, resultHash);
		}
		if (!this.taskExecutor.abortedTaskExecution()) { // this means that this executor is actually the one that is going to abort the others...
			// Clean up all "executing" tasks
			// Multimap<Task, LocationBean> dataToRemove = job.taskDataToRemove(job.currentProcedureIndex());
			// this.removeDataFromDHT(dataToRemove);
			this.dhtConnectionProvider.broadcastFinishedAllTasks(job);
			this.messageConsumer.updateMessagesFromJobUpdate(job.id());

		}
		awaitMessageConsumerCompletion();

		printResults(job);

		// Create new tasks

		// submit new tasks

	}

	public void abortExecution(Job job) {
		if (job.id().equals(jobs.peek().id())) {
			this.taskExecutor.abortTaskExecution();
		}

	}

	private void removeDataFromDHT(final Multimap<Task, LocationBean> dataToRemove) {
		for (final Task task : dataToRemove.keySet()) {
			Collection<LocationBean> locationBeans = dataToRemove.get(task);
			for (final LocationBean b : locationBeans) {
				new Thread(new Runnable() {

					@Override
					public void run() {
						dhtConnectionProvider.removeTaskResultsFor(task, b);
					}

				}).start();
			}
		}
	}

	private void awaitMessageConsumerCompletion() {
		while (this.messageConsumer.isBusy()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void printResults(Job job) {
		// Only printing
		BlockingQueue<Task> ts = job.tasks(job.currentProcedureIndex());
		for (Task t : ts) {
			logger.info(t.finalDataLocation().procedure().getClass().getSimpleName() + ": " + t.finalDataLocation().dataLocation().first() + ", "
					+ t.finalDataLocation().dataLocation().second());
		}
	}

	public boolean isExecutionAborted() {
		return this.taskExecutor.abortedTaskExecution();
	}

	// End Execution

}

// awaitMessageConsumerCompletion();
// Task Comparison & new task creation
// tasks = new ArrayList<Task>(job.tasks(job.currentProcedureIndex())); // Hypothetically, can also reuse it directly from above?
//
// task = null;
// //TODO send result hash with broadcast
// while ((task = this.taskResultComparisonScheduler.schedule(tasks)) != null && !this.taskResultComparator.abortedTaskComparisons()
// && canExecute()) {
// this.dhtConnectionProvider.broadcastExecutingCompareTaskResults(task);
// Map<Tuple<PeerAddress, Integer>, Multimap<Object, Object>> data = this.getDataToCompare(task);
// Tuple<PeerAddress, Integer> taskResultEvaluationResult = this.taskResultComparator.compareTaskResults(data);
// task.finalDataLocation(taskResultEvaluationResult);
// this.dhtConnectionProvider.broadcastFinishedTaskComparison(task);
// }
// if (!this.taskResultComparator.abortedTaskComparisons()) { // this means that this executor is actually the one that is going to abort the
// // others...
// this.dhtConnectionProvider.broadcastFinishedAllTaskComparisons(job);
// // abortTaskExecution();
// logger.info("This executor aborts task comparisons!");
// this.messageConsumer.updateMessagesFromJobUpdate(job.id(), BCMessageStatus.FINISHED_ALL_TASK_COMPARIONS);
// printResults(job);
// }
//
// awaitMessageConsumerCompletion();
// Clean up data locations of unused tasks
// private Map<Tuple<PeerAddress, Integer>, Multimap<Object, Object>> getDataToCompare(Task task) {
// Map<Tuple<PeerAddress, Integer>, Multimap<Object, Object>> dataForEachResult = new HashMap<Tuple<PeerAddress, Integer>, Multimap<Object,
// Object>>();
// ArrayList<PeerAddress> allAssignedPeers = task.allAssignedPeers();
// for (PeerAddress p : allAssignedPeers) {
// ArrayList<BCMessageStatus> statiForPeer = task.statiForPeer(p);
// for (int i = 0; i < statiForPeer.size(); ++i) {
// if (statiForPeer.get(i).equals(BCMessageStatus.FINISHED_TASK)) { // just to make sure, double check, should never be the case anyways!
// Tuple<PeerAddress, Integer> dataLocationTuple = Tuple.create(p, i);
// Multimap<Object, Object> taskDataForLocationTuple = dhtConnectionProvider.getTaskData(task,
// LocationBean.create(dataLocationTuple, task.procedure()));
// dataForEachResult.put(dataLocationTuple, taskDataForLocationTuple);
// }
// }
// }
// return dataForEachResult;
// }