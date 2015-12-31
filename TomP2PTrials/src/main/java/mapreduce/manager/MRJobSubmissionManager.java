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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.Tasks;
import mapreduce.execution.task.taskdatacomposing.ITaskDataComposer;
import mapreduce.execution.task.taskdatacomposing.MaxFileSizeTaskDataComposer;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.MRJobSubmissionManagerMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasting.broadcastmessages.jobmessages.JobDistributedBCMessage;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileUtils;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureListener;

public class MRJobSubmissionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmissionManager.class);
	private static final ITaskDataComposer DEFAULT_TASK_DATA_COMPOSER = MaxFileSizeTaskDataComposer.create();

	private IDHTConnectionProvider dhtConnectionProvider;
	private MRJobSubmissionManagerMessageConsumer messageConsumer;
	private ITaskDataComposer taskDataComposer;
	private String id;
	private String resultDomain;

	private MRJobSubmissionManager(IDHTConnectionProvider dhtConnectionProvider, List<Job> jobs) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.dhtConnectionProvider(dhtConnectionProvider.owner(this.id));
		this.messageConsumer = MRJobSubmissionManagerMessageConsumer.newInstance(this, jobs).canTake(true);
		new Thread(messageConsumer).start();
		dhtConnectionProvider.addMessageQueueToBroadcastHandler(messageConsumer.queue());
	}

	public static MRJobSubmissionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobSubmissionManager(dhtConnectionProvider, Collections.synchronizedList(new ArrayList<>()))
				.taskComposer(DEFAULT_TASK_DATA_COMPOSER);
	}

	public void connect() {
		dhtConnectionProvider.connect();
	}

	/**
	 * The idea is that for each key/value pair, a broadcast is done that a new task is available for a certain job. The tasks then can be pulled from
	 * the DHT. If a submission fails here, the whole job is aborted because not the whole data could be sent (this is different from when a job
	 * executor fails, as then the job simply is marked as failed, cleaned up, and may be pulled again).
	 * 
	 * @param job
	 * @return
	 */
	public void submit(final Job job) {
		taskDataComposer.maxFileSize(job.maxFileSize());

		List<String> keysFilePaths = new ArrayList<String>();
		File file = new File(job.fileInputFolderPath());
		FileUtils.INSTANCE.getFiles(file, keysFilePaths);
		// List<Task> tasks = new ArrayList<>();
		// Adding keys and data to task executor domain
		for (String keyfilePath : keysFilePaths) {
			Path path = Paths.get(keyfilePath);
			Charset charset = Charset.forName(taskDataComposer.fileEncoding());

			int filePartCounter = 0;
			try (BufferedReader reader = Files.newBufferedReader(path, charset)) {

				String line = null;
				while ((line = reader.readLine()) != null) {
					String taskValues = taskDataComposer.append(line);
					if (taskValues != null) {
						filePartCounter = addDataToDHT(job, file, keyfilePath, taskValues, filePartCounter);
					}
				}
			} catch (IOException x) {
				System.err.format("IOException: %s%n", x);
			}

			if (taskDataComposer.currentValues() != null) {
				filePartCounter = addDataToDHT(job, file, keyfilePath, taskDataComposer.currentValues(), filePartCounter);
				taskDataComposer.reset();
			}
		}

	}

	private int addDataToDHT(final Job job, File file, String keyfilePath, String taskValue, int filePartCounter) {
		String fileName = keyfilePath.replace(file.getPath(), "").replace(".", "").replace("\\", "");
		String taskKey = fileName + "_" + filePartCounter++;

		ProcedureInformation pI = job.previousProcedure();
		
		Task task = Task.create(taskKey, pI.jobProcedureDomain());
		for (int i = 0; i < Tasks.bestOfMaxNrOfFinishedWorkersWithSameResultHash(job.maxNrOfFinishedWorkersPerTask()); ++i) {
			Tasks.updateStati(task, TaskResult.create().sender(id).status(BCMessageStatus.EXECUTING_TASK), job.maxNrOfFinishedWorkersPerTask());
			Tasks.updateStati(task, TaskResult.create().sender(id).status(BCMessageStatus.FINISHED_TASK), job.maxNrOfFinishedWorkersPerTask());
		}
		Tuple<String, Integer> taskExecutor = Tuple.create(id, task.executingPeers().get(id).size() - 1);

		String jobProcedureDomainString = pI.jobProcedureDomainString();

		String taskExecutorDomainConcatenation = task.concatenationString(taskExecutor);

		// Add <key, value, taskExecutorDomain>
		dhtConnectionProvider.add(taskKey, taskValue, taskExecutorDomainConcatenation, true).addListener(new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					// Add <key, taskExecutorDomainPart, jobProcedureDomain> to collect all keys
					dhtConnectionProvider.add(task.id(), taskExecutor, jobProcedureDomainString, false)
							.addListener(new BaseFutureListener<FuturePut>() {

						@Override
						public void operationComplete(FuturePut future) throws Exception {
							if (future.isSuccess()) {
								// Add <PROCEDURE_KEYS, key, jobProcedureDomain> to collect all domains for all keys
								dhtConnectionProvider.add(DomainProvider.PROCEDURE_KEYS, task.id(), jobProcedureDomainString, false)
										.addListener(new BaseFutureListener<FuturePut>() {

									@Override
									public void operationComplete(FuturePut future) throws Exception {
										if (future.isSuccess()) {
											logger.info("Successfully added data. Broadcasting job.");

											JobDistributedBCMessage message = dhtConnectionProvider.broadcastNewJob(job);
											messageConsumer.queue().add(message);
										} else {
											logger.warn(future.failedReason());
										}
									}

									@Override
									public void exceptionCaught(Throwable t) throws Exception {
										logger.warn("Exception thrown", t);
									}
								});
							} else {
								logger.warn(future.failedReason());
							}
						}

						@Override
						public void exceptionCaught(Throwable t) throws Exception {
							logger.warn("Exception thrown", t);
						}
					});
				} else {
					logger.warn(future.failedReason());
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.warn("Exception thrown", t);
			}
		});

		return filePartCounter;
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
		logger.info("shutdown()::1::disconnecting jobSubmissionManager from DHT. Bye...");
		this.dhtConnectionProvider.shutdown();
	}

	public String id() {
		return this.id;
	}

	public void finishedJob(String resultDomain) {
		logger.info("Received final job procedure domain to get the data from: " + resultDomain);
		this.resultDomain = resultDomain;
	}

}
