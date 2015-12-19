package mapreduce.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.taskdatacomposing.ITaskDataComposer;
import mapreduce.execution.task.taskdatacomposing.MaxFileSizeTaskDataComposer;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobSubmitterMessageConsumer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.FileUtils;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;

public class MRJobSubmissionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmissionManager.class);
	private static final ITaskDataComposer DEFAULT_TASK_DATA_COMPOSER = MaxFileSizeTaskDataComposer.create();

	private IDHTConnectionProvider dhtConnectionProvider;
	private MRJobSubmitterMessageConsumer messageConsumer;
	private ITaskDataComposer taskDataComposer;
	private String id;

	private MRJobSubmissionManager(IDHTConnectionProvider dhtConnectionProvider, CopyOnWriteArrayList<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobSubmitterMessageConsumer.newInstance(id, jobs).canTake(true);
		new Thread(messageConsumer).start();
		dhtConnectionProvider.broadcastHandler().queue(messageConsumer.queue());
	}

	public static MRJobSubmissionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobSubmissionManager(dhtConnectionProvider, new CopyOnWriteArrayList<Job>()).taskComposer(DEFAULT_TASK_DATA_COMPOSER);
	}

	/**
	 * 
	 * @param job
	 * @return
	 */
	public void submit(final Job job) {
		dhtConnectionProvider.connect();

		List<String> keysFilePaths = new ArrayList<String>();

		FileUtils.INSTANCE.getFiles(new File(job.fileInputFolderPath()), keysFilePaths);
		List<Boolean> taskDataSubmitted = Collections.synchronizedList(new ArrayList<>());
		for (String keyfilePath : keysFilePaths) {
			Path file = Paths.get(keyfilePath);

			Charset charset = Charset.forName(taskDataComposer.fileEncoding());
			try (BufferedReader reader = Files.newBufferedReader(file, charset)) {
				String line = null;
				while ((line = reader.readLine()) != null) {
					int domainCounter = 0;
					String taskKey = keyfilePath + "_" + domainCounter;
					String value = null;
					if ((value = taskDataComposer.append(line)) != null) {
						int taskDataSubmittedIndexToSet = -1;
						synchronized (taskDataSubmitted) {
							taskDataSubmittedIndexToSet = taskDataSubmitted.size();// just convenience 'cos I'm gonna add the next boolean at this
																					// position
							taskDataSubmitted.add(false);
						}
						if (value != null) {
							Task task = Task.newInstance(taskKey, job.id()).finalDataLocation(Tuple.create(dhtConnectionProvider.peerAddress(), 0));
					 
							dhtConnectionProvider.addData(job, task, value, taskDataSubmitted, taskDataSubmittedIndexToSet); // Do everything in here!!!
						}
						++domainCounter;
					}
				}
			} catch (IOException x) {
				System.err.format("IOException: %s%n", x);
			}
		}
		while (taskDataSubmitted.contains(false)) {
			try {
				Thread.sleep(100);// Needs to sleep, because job can only be broadcasted once all data is available
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		dhtConnectionProvider.broadcastNewJob(job);

	}

	public MRJobSubmissionManager taskComposer(ITaskDataComposer taskDataComposer) {
		this.taskDataComposer = taskDataComposer;
		return this;
	}

	public MRJobSubmissionManager dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public void shutdown() {
		this.dhtConnectionProvider.shutdown();
	}

	public String id() {
		return this.id;
	}

}
