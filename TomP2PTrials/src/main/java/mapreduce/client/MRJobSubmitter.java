package mapreduce.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.MessageConsumer;
import mapreduce.execution.broadcasthandler.broadcastobserver.IBroadcastListener;
import mapreduce.execution.datasplitting.ITaskSplitter;
import mapreduce.execution.datasplitting.MaxFileSizeTaskSplitter;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;

public class MRJobSubmitter implements IBroadcastListener {
	private static final ITaskSplitter DEFAULT_TASK_SPLITTER = MaxFileSizeTaskSplitter.newMaxFileSizeTaskSplitter();
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmitter.class);
	private IDHTConnectionProvider dhtConnectionProvider;
	private ITaskSplitter taskSplitter;

	private MRJobSubmitter(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);

		MessageConsumer messageConsumer = MessageConsumer.newMessageConsumer(jobs).canTake(true);
		new Thread(messageConsumer).start();
		dhtConnectionProvider.broadcastHandler().queue(messageConsumer.queue());
	}

	public static MRJobSubmitter newMapReduceJobSubmitter(IDHTConnectionProvider dhtConnectionProvider, BlockingQueue<Job> jobs) {
		return new MRJobSubmitter(dhtConnectionProvider, jobs);
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
		for (final Task task : job.tasksFor(job.nextProcedure())) {
			server.submit(new Runnable() {

				@Override
				public void run() {
					for (Object key : task.keys()) {
						try {
							String filePath = (String) key;
							BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
							String line = null;
							String lines = "";
							while ((line = reader.readLine()) != null) {
								lines += line + "\n";
							}
							reader.close();
							dhtConnectionProvider.addDataForTask(task.id(), filePath, lines);
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

	@Override
	public void inform(IBCMessage bcMessage) {
		System.out.println("MRJobSubmitter received BC message with status: " + bcMessage.status());
	}

	public void shutdown() {
		this.dhtConnectionProvider.shutdown();
	}

}
