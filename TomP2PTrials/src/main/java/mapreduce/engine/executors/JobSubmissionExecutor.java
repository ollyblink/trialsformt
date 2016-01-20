package mapreduce.engine.executors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;
import mapreduce.execution.tasks.taskdatacomposing.ITaskDataComposer;
import mapreduce.execution.tasks.taskdatacomposing.MaxFileSizeTaskDataComposer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileUtils;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class JobSubmissionExecutor extends AbstractExecutor {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionExecutor.class);
	private static final MaxFileSizeTaskDataComposer DEFAULT_TASK_DATA_COMPOSER = MaxFileSizeTaskDataComposer.create();
	private static final String DEFAULT_OUTPUT_FOLDER = System.getProperty("user.dir") + "/tmp/";
	private static final int DEFAULT_NR_OF_SUBMISSIONS = 1;
	private ITaskDataComposer taskDataComposer = DEFAULT_TASK_DATA_COMPOSER;
	private String outputFolder = DEFAULT_OUTPUT_FOLDER;
	/** How many times should a job be tried to be submitted before it is aborted? */
	private int maxNrOfSubmissions = DEFAULT_NR_OF_SUBMISSIONS;
	private Set<Job> submittedJobs = SyncedCollectionProvider.syncedHashSet();

	private JobSubmissionExecutor() {
		super(IDCreator.INSTANCE.createTimeRandomID(JobSubmissionExecutor.class.getSimpleName()));
	}

	public static JobSubmissionExecutor create() {
		return new JobSubmissionExecutor();
	}

	// Getter/Setter
	public JobSubmissionExecutor taskComposer(ITaskDataComposer taskDataComposer) {
		this.taskDataComposer = taskDataComposer;
		return this;
	}

	public JobSubmissionExecutor outputFolder(String outputFolder) {
		if (!new File(outputFolder).exists()) {
			new File(outputFolder).mkdir();
		}
		this.outputFolder = outputFolder;
		return this;
	}

	public JobSubmissionExecutor maxNrOfSubmissions(int maxNrOfSubmissions) {
		this.maxNrOfSubmissions = maxNrOfSubmissions;
		return this;
	}

	public void resubmitJobIfPossible(Job job) {
		if (job.incrementSubmissionCounter() < maxNrOfSubmissions) {
			submit(job);
		} else {
			System.err.println("Job submission aborted. Job: " + job);
		}
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
		taskDataComposer.splitValue("\n").maxFileSize(job.maxFileSize());

		List<String> keysFilePaths = filePaths(job);

		Procedure procedure = job.currentProcedure();
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), job.submissionCount(), id,
				procedure.executable().getClass().getSimpleName(), procedure.procedureIndex());
		procedure.dataInputDomain(JobProcedureDomain.create(job.id(), job.submissionCount(), id, DomainProvider.INITIAL_PROCEDURE, -1)
				.expectedNrOfFiles(estimatedNrOfFiles(job, keysFilePaths))).addOutputDomain(outputJPD);
		List<FutureDone<List<FuturePut>>> allDoneListeners = SyncedCollectionProvider.syncedArrayList();

		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly().addListener(new BaseFutureAdapter<BaseFuture>() {

			@Override
			public void operationComplete(BaseFuture future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Successfully submitted job " + job);
					submittedJobs.add(job);
					for (String keyfilePath : keysFilePaths) {
						readFile(job, procedure, outputJPD, allDoneListeners, keyfilePath);
					}
				} else {
					logger.info("No success on submitting job: " + job);
				}
			}

		});

	}

	private void readFile(final Job job, Procedure procedure, JobProcedureDomain outputJPD, List<FutureDone<List<FuturePut>>> allDoneListeners,
			String keyfilePath) {
		Path path = Paths.get(keyfilePath);
		Charset charset = Charset.forName(taskDataComposer.fileEncoding());

		int filePartCounter = 0;
		try (BufferedReader reader = Files.newBufferedReader(path, charset)) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				line = taskDataComposer.remainingData() + "\n" + line;
				List<String> splitToSize = taskDataComposer.splitToSize(line);

				for (String split : splitToSize) {
					submitInternally(job, procedure, outputJPD, allDoneListeners, keyfilePath, filePartCounter++, split);
				}
			}
			if (taskDataComposer.remainingData().length() > 0) {
				submitInternally(job, procedure, outputJPD, allDoneListeners, keyfilePath, filePartCounter, taskDataComposer.remainingData());
			}
		} catch (IOException x) {
			logger.info("external submit(): IOException: %s%n", x);
		}
	}

	private void submitInternally(final Job job, Procedure procedure, JobProcedureDomain outputJPD,
			List<FutureDone<List<FuturePut>>> allDoneListeners, String keyfilePath, int filePartCounter, String vals) {
		Collection<Object> values = new ArrayList<>();
		values.add(vals);
		Task task = Task.create(new File(keyfilePath).getName() + "_" + filePartCounter, id);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), id, task.newStatusIndex(), outputJPD);
		IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);

		logger.info("internal submit(): Put split: <" + task.key() + ", \"" + vals + "\">");
		((IExecutable) procedure.executable()).process(task.key(), values, context);
		allDoneListeners.add(Futures.whenAllSuccess(context.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {
			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					outputETD.resultHash(context.resultHash());
					IBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(outputETD,
							procedure.dataInputDomain().nrOfFinishedTasks(procedure.nrOfFinishedTasks()));
					// dhtConnectionProvider.broadcastHandler().processMessage(msg, job);
					dhtConnectionProvider.broadcastCompletion(msg);
					logger.info("Successfully broadcasted TaskCompletedBCMessage for task " + task);
				} else {
					logger.warn("No success on task execution. Reason: " + future.failedReason());
				}
			}

		}));
	}

	private List<String> filePaths(final Job job) {
		List<String> keysFilePaths = new ArrayList<String>();

		FileUtils.INSTANCE.getFiles(new File(job.fileInputFolderPath()), keysFilePaths);
		return keysFilePaths;
	}

	/**
	 * Tries to give an initial guess of how many files there are going to be (this calculation is solely based on the overall file size. It may be
	 * that splitting the file reduces the file sizes and thus, fewer files are actually transferred in the process than were expected
	 * 
	 * @param job
	 * @param keysFilePaths
	 * @return
	 */
	private int estimatedNrOfFiles(final Job job, List<String> keysFilePaths) {
		int nrOfFiles = 0;
		for (String fileName : keysFilePaths) {
			long fileSize = new File(fileName).length();
			nrOfFiles += (int) (fileSize / job.maxFileSize().value());
			if (fileSize % job.maxFileSize().value() > 0) {
				++nrOfFiles;
			}
		}
		return nrOfFiles;
	}

	public void retrieveDataOfFInishedJob(JobProcedureDomain resultDomain) {
		taskDataComposer.splitValue(",");
		List<FutureGet> getKeys = SyncedCollectionProvider.syncedArrayList();
		List<FutureGet> getDomains = SyncedCollectionProvider.syncedArrayList();

		ListMultimap<String, ExecutorTaskDomain> keyDomains = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());

		getKeys.add(dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, resultDomain.toString())
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						logger.info("Job Proc domain: " + resultDomain);
						if (future.isSuccess()) {
							try {
								for (Number640 keyNumber : future.dataMap().keySet()) {
									String key = (String) future.dataMap().get(keyNumber).object();
									logger.info("Key: " + key);
									logger.info("Get <" + key + "," + resultDomain.toString() + ">");
									getDomains.add(dhtConnectionProvider.getAll(key, resultDomain.toString())
											.addListener(new BaseFutureAdapter<FutureGet>() {

										@Override
										public void operationComplete(FutureGet future) throws Exception {
											if (future.isSuccess()) {
												try {
													for (Number640 executorTaskDomainNumber : future.dataMap().keySet()) {
														ExecutorTaskDomain inputDomain = (ExecutorTaskDomain) future.dataMap()
																.get(executorTaskDomainNumber).object();
														logger.info("inputDomain: " + inputDomain);
														keyDomains.put(key, inputDomain);

													}
												} catch (IOException e) {
													logger.info("failed");
												}
											} else {
												logger.info("failed");
											}
										}

									}));

								}
							} catch (IOException e) {
								logger.info("failed");
							}
						} else {
							logger.info("failed");
						}
					}
				}));

		List<FutureGet> getValues = SyncedCollectionProvider.syncedArrayList();
		Futures.whenAllSuccess(getKeys).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					Futures.whenAllSuccess(getDomains).addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								for (String key : keyDomains.keySet()) {
									getValues.add(dhtConnectionProvider.getAll(key, keyDomains.get(key).toString())
											.addListener(new BaseFutureAdapter<FutureGet>() {

										@Override
										public void operationComplete(FutureGet future) throws Exception {
											if (future.isSuccess()) {
												try {
													write(key.toString(), true);
													for (Number640 valueNr : future.dataMap().keySet()) {
														Object value = ((Value) future.dataMap().get(valueNr).object()).value();
														write(value.toString(), false);
													}
												} catch (IOException e) {
													logger.info("failed");
												}
											} else {
												logger.info("failed");
											}
										}

									}));
								}
							} else {

							}
						}

					});
				} else {
					// Try again
				}
			}

		});
		Futures.whenAllSuccess(getValues).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Successfully wrote data to file system at location: " + outputFolder);
					logger.info("Marking job " + resultDomain.jobId() + " as finished.");
					markAsRetrieved(resultDomain.jobId());
				}
			}
		});
	}

	private void write(String toAppend, boolean isKey) {
		if (isKey) {
			toAppend = "\n" + toAppend;
		}
		// String values = taskDataComposer.append(toAppend);
		// if (values != null) {
		Path file = Paths.get(outputFolder);
		Charset charset = Charset.forName(taskDataComposer.fileEncoding());
		try (BufferedWriter writer = Files.newBufferedWriter(file, charset)) {
			writer.write(toAppend);
			writer.flush();
		} catch (IOException x) {
			System.err.format("IOException: %s%n", x);
		}
		// }

	}

	public void markAsRetrieved(String jobId) {
		for (Job job : submittedJobs) {
			if (job.id().equals(jobId)) {
				job.isRetrieved(true);
			}
		}
	}

	@Override
	public JobSubmissionExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		return (JobSubmissionExecutor) super.dhtConnectionProvider(dhtConnectionProvider);
	}

	public boolean submittedJob(Job job) {
		return submittedJobs.contains(job);
	}

	public boolean jobIsRetrieved(Job job) {
		if (submittedJobs.contains(job)) {
			synchronized (submittedJobs) {
				for (Job j : submittedJobs) {
					if (j.equals(job)) {
						return j.isRetrieved();
					}
				}
			}
		}
		return false;
	}

	public Job job(String jobId) {
		synchronized (submittedJobs) {
			for (Job j : submittedJobs) {
				if (j.id().equals(jobId)) {
					return j;
				}
			}
		}
		return null;
	}
}
