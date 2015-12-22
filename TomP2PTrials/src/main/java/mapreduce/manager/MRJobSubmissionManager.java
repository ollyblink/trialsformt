package mapreduce.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.taskdatacomposing.ITaskDataComposer;
import mapreduce.execution.task.taskdatacomposing.MaxFileSizeTaskDataComposer;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobSubmissionManagerMessageConsumer;
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

	private MRJobSubmissionManager(IDHTConnectionProvider dhtConnectionProvider, CopyOnWriteArrayList<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobSubmissionManagerMessageConsumer.newInstance(id, jobs).canTake(true);
		new Thread(messageConsumer).start();
		dhtConnectionProvider.broadcastHandler().queue(messageConsumer.queue());
	}

	public static MRJobSubmissionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobSubmissionManager(dhtConnectionProvider, new CopyOnWriteArrayList<Job>()).taskComposer(DEFAULT_TASK_DATA_COMPOSER);
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
		dhtConnectionProvider.connect();

		List<String> keysFilePaths = new ArrayList<String>();

		FileUtils.INSTANCE.getFiles(new File(job.fileInputFolderPath()), keysFilePaths);
		List<Task> tasks = new ArrayList<>();
		// Adding keys and data to task executor domain
		for (String keyfilePath : keysFilePaths) {
			Path file = Paths.get(keyfilePath);

			Charset charset = Charset.forName(taskDataComposer.fileEncoding());

			try (BufferedReader reader = Files.newBufferedReader(file, charset)) {
				String line = null;
				while ((line = reader.readLine()) != null) {
					String taskKey = keyfilePath;// + "_" + domainCounter;
					String taskValue = line + "\n";
					Task task = Task.newInstance(keyfilePath, job.id()).finalDataLocation(Tuple.create(dhtConnectionProvider.peerAddress(), 0));
					String taskExecutorDomain = DomainProvider.INSTANCE.executorTaskDomain(task, task.finalDataLocation());
					tasks.add(task);
					dhtConnectionProvider.add(taskKey, taskValue, taskExecutorDomain, true).addListener(new BaseFutureListener<FuturePut>() {

						@Override
						public void operationComplete(FuturePut future) throws Exception {
							if (future.isSuccess()) {
								String jobProcedureDomain = DomainProvider.INSTANCE.jobProcedureDomain(job);
								dhtConnectionProvider.add(task.id(), taskExecutorDomain, jobProcedureDomain, false)
										.addListener(new BaseFutureListener<FuturePut>() {

									@Override
									public void operationComplete(FuturePut future) throws Exception {
										if (future.isSuccess()) {
											dhtConnectionProvider.add("PROCEDURE_KEYS", task.id(), jobProcedureDomain, false)
													.addListener(new BaseFutureListener<FuturePut>() {

												@Override
												public void operationComplete(FuturePut future) throws Exception {
													if (future.isSuccess()) {
														logger.info("Successfully added data. Broadcasting job.");
														dhtConnectionProvider.broadcastNewJob(job);
													} else {
														logger.warn(future.failedReason());
														dhtConnectionProvider.broadcastJobFailed(job);
													}
												}

												@Override
												public void exceptionCaught(Throwable t) throws Exception {
													logger.warn("Exception thrown", t);
													dhtConnectionProvider.broadcastJobFailed(job);
												}
											});
										} else {
											logger.warn(future.failedReason());
											dhtConnectionProvider.broadcastJobFailed(job);
										}
									}

									@Override
									public void exceptionCaught(Throwable t) throws Exception {
										logger.warn("Exception thrown", t);
										dhtConnectionProvider.broadcastJobFailed(job);
									}
								});
							} else {
								logger.warn(future.failedReason());
								dhtConnectionProvider.broadcastJobFailed(job);
							}
						}

						@Override
						public void exceptionCaught(Throwable t) throws Exception {
							logger.warn("Exception thrown", t);
							dhtConnectionProvider.broadcastJobFailed(job);
						}
					});
				}
			} catch (IOException x) {
				System.err.format("IOException: %s%n", x);
			}
		}

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
