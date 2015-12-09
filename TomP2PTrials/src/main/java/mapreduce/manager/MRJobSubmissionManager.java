package mapreduce.manager;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.tasksplitting.ITaskSplitter;
import mapreduce.execution.task.tasksplitting.MaxFileSizeTaskSplitter;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobSubmitterMessageConsumer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.storage.LocationBean;
import mapreduce.utils.FileUtils;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;

public class MRJobSubmissionManager {
	private static final ITaskSplitter DEFAULT_TASK_SPLITTER = MaxFileSizeTaskSplitter.newInstance();
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmissionManager.class);
	private IDHTConnectionProvider dhtConnectionProvider;
	private ITaskSplitter taskSplitter;
	private MRJobSubmitterMessageConsumer messageConsumer;
	private String id;

	private MRJobSubmissionManager(IDHTConnectionProvider dhtConnectionProvider, CopyOnWriteArrayList<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobSubmitterMessageConsumer.newInstance(id, jobs).canTake(true);
		new Thread(messageConsumer).start();
		dhtConnectionProvider.broadcastHandler().queue(messageConsumer.queue());
	}

	public static MRJobSubmissionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobSubmissionManager(dhtConnectionProvider, new CopyOnWriteArrayList<Job>());
	}

	/**
	 * 
	 * @param job
	 * @return
	 */
	public void submit(final Job job, final boolean awaitOnAdd) {
		dhtConnectionProvider().connect();
		logger.warn("Connected.");
		// Split into specified file sizes
		taskSplitter().split(job);
		logger.warn("Splitted tasks.");

		ExecutorService server = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for (final Task task : job.firstTasks()) {
			// Set the initial data location for the first task set
			task.initialDataLocation(LocationBean.create(Tuple.create(this.dhtConnectionProvider().peerAddress(), 0), task.procedure()));

			server.submit(new Runnable() {

				@Override
				public void run() {

					for (final Comparable key : taskSplitter.keysForEachTask().get(task)) {
						try {
							String filePath = (String) key;
							String lines = FileUtils.INSTANCE.readLines(filePath);
							dhtConnectionProvider.addTaskData(task, filePath, lines, awaitOnAdd);
							logger.warn("Added file with path " + filePath);
						} catch (IOException e) {
							logger.error("Exception", e);
						}
					}
				}

			});
		}
		server.shutdown();
		while (!server.isTerminated()) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		dhtConnectionProvider.broadcastNewJob(job);
		logger.info("broadcased job");
	}

	public MRJobSubmissionManager dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public MRJobSubmissionManager taskSplitter(ITaskSplitter taskSplitter) {
		this.taskSplitter = taskSplitter;
		return this;
	}

	public ITaskSplitter taskSplitter() {
		if (this.taskSplitter == null) {
			this.taskSplitter = DEFAULT_TASK_SPLITTER;
		}
		return this.taskSplitter;
	}

	public void shutdown() {
		this.dhtConnectionProvider.shutdown();
	}

	public String id() {
		return this.id;
	}

}
