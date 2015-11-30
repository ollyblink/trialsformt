package mapreduce.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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

public class MRJobSubmitter {
	private static final ITaskSplitter DEFAULT_TASK_SPLITTER = MaxFileSizeTaskSplitter.newMaxFileSizeTaskSplitter();
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmitter.class);
	private IDHTConnectionProvider dhtConnectionProvider;
	private ITaskSplitter taskSplitter;
	private MRJobSubmitterMessageConsumer messageConsumer;
	private String id;

	private MRJobSubmitter(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobSubmitterMessageConsumer.newMRJobSubmitterMessageConsumer(id, jobs).canTake(true);
		new Thread(messageConsumer).start();
		dhtConnectionProvider.broadcastHandler().queue(messageConsumer.queue());
	}

	public static MRJobSubmitter newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobSubmitter(dhtConnectionProvider, new LinkedBlockingQueue<Job>());
	}

	/**
	 * 
	 * @param job
	 * @return
	 */
	public void submit(final Job job) {
		dhtConnectionProvider().connect();
		logger.warn("Connected.");
		// Split into specified file sizes
		taskSplitter().split(job);
		logger.warn("Splitted tasks.");

		ExecutorService server = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for (final Task task : job.firstTasks()) {
			// The next 2 lines are needed to keep the addDataForTask method generic and such that the job submitter acts similar to a job executor
			// --------------------------------------------------------------
			task.finalPeerAddress(this.dhtConnectionProvider().peerAddress());
			task.finalJobStatusIndex(0);
			// --------------------------------------------------------------

			server.submit(new Runnable() {

				@Override
				public void run() {

					for (final Comparable key : taskSplitter.keysForEachTask().get(task)) {
						try {
							String filePath = (String) key;
							String lines = FileUtils.INSTANCE.readLines(filePath);
							dhtConnectionProvider.addDataForTask(task, filePath, lines);
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

	

	public MRJobSubmitter dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public MRJobSubmitter taskSplitter(ITaskSplitter taskSplitter) {
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

	public static void main(String[] args) {
		Set<Object> set = new TreeSet<Object>();
		List<String> vals = new ArrayList<String>();
		vals.add("Hello");
		vals.add("Hello");
		vals.add("Hello");
		set.addAll(vals);
		System.err.println(set.size());
	}
}
