package mapreduce.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.messageconsumer.MRJobSubmitterMessageConsumer;
import mapreduce.execution.datasplitting.ITaskSplitter;
import mapreduce.execution.datasplitting.MaxFileSizeTaskSplitter;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.FileUtils;
import mapreduce.utils.IDCreator;

public class MRJobSubmissionManager {
	private static final ITaskSplitter DEFAULT_TASK_SPLITTER = MaxFileSizeTaskSplitter.newMaxFileSizeTaskSplitter();
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmissionManager.class);
	private IDHTConnectionProvider dhtConnectionProvider;
	private ITaskSplitter taskSplitter;
	private MRJobSubmitterMessageConsumer messageConsumer;
	private String id;

	private MRJobSubmissionManager(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobSubmitterMessageConsumer.newInstance(id, jobs).canTake(true);
		new Thread(messageConsumer).start();
		dhtConnectionProvider.broadcastHandler().queue(messageConsumer.queue());
	}

	public static MRJobSubmissionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobSubmissionManager(dhtConnectionProvider, new LinkedBlockingQueue<Job>());
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
			// The next 2 lines are needed to keep the addDataForTask method generic and such that the job submitter acts similar to a job executor
			// --------------------------------------------------------------
			task.dataLocationHashPeerAddress(this.dhtConnectionProvider().peerAddress());
			task.dataLocationHashJobStatusIndex(0);
			// --------------------------------------------------------------

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
