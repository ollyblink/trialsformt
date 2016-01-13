package mapreduce.engine.executor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import mapreduce.engine.broadcasting.CompletedBCMessage;
import mapreduce.engine.messageconsumer.MRJobSubmissionManagerMessageConsumer;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.taskdatacomposing.ITaskDataComposer;
import mapreduce.execution.task.taskdatacomposing.MaxFileSizeTaskDataComposer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileUtils;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class MRJobSubmissionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmissionManager.class);
	private static final ITaskDataComposer DEFAULT_TASK_DATA_COMPOSER = MaxFileSizeTaskDataComposer.create();

	private IDHTConnectionProvider dhtConnectionProvider;
	private ITaskDataComposer taskDataComposer;
	private String id;
	private JobProcedureDomain resultDomain;
	private String outputFolder;

	private MRJobSubmissionManager(IDHTConnectionProvider dhtConnectionProvider) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.dhtConnectionProvider = dhtConnectionProvider.executor(this.id);
		this.dhtConnectionProvider.broadcastHandler().messageConsumer(MRJobSubmissionManagerMessageConsumer.create(this));
	}

	public static MRJobSubmissionManager create(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobSubmissionManager(dhtConnectionProvider).taskComposer(DEFAULT_TASK_DATA_COMPOSER)
				.outputFolder(System.getProperty("user.dir") + "/tmp/");
	}

	public MRJobSubmissionManager outputFolder(String outputFolder) {
		if (!new File(outputFolder).exists()) {
			new File(outputFolder).mkdir();
		}
		this.outputFolder = outputFolder;
		return this;
	}

	public void connect() {
		try {
			dhtConnectionProvider.connect();
		} catch (Exception e) {
			e.printStackTrace();
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

		List<String> keysFilePaths = new ArrayList<String>();
		File file = new File(job.fileInputFolderPath());
		FileUtils.INSTANCE.getFiles(file, keysFilePaths);

		// Get the number of files to be expected
		// long overallFileSizes = 0;
		int nrOfFiles = 0;
		for (String fileName : keysFilePaths) {
			long fileSize = new File(fileName).length();
			nrOfFiles += (int) (fileSize / job.maxFileSize().value());
			if (fileSize % job.maxFileSize().value() > 0) {
				++nrOfFiles;
			}
		} 

		JobProcedureDomain inputDomain = JobProcedureDomain.create(job.id(), id, "BEFORE_START", -1).tasksSize(nrOfFiles);
		JobProcedureDomain outputDomain = JobProcedureDomain.create(job.id(), id, job.currentProcedure().executable().getClass().getSimpleName(),
				job.currentProcedure().procedureIndex());
		job.currentProcedure().nrOfSameResultHash(1).dataInputDomain(inputDomain).addOutputDomain(outputDomain);

		List<FuturePut> futurePutValues = SyncedCollectionProvider.syncedArrayList();
		List<FuturePut> futurePutKeys = SyncedCollectionProvider.syncedArrayList();

		for (String keyfilePath : keysFilePaths) {
			Path path = Paths.get(keyfilePath);
			Charset charset = Charset.forName(taskDataComposer.fileEncoding());

			int filePartCounter = 0;
			try (BufferedReader reader = Files.newBufferedReader(path, charset)) {

				String line = null;
				while ((line = reader.readLine()) != null) {
					String taskValues = taskDataComposer.append(line);
					if (taskValues != null) {
						dhtConnectionProvider.broadcastHandler().s
						// filePartCounter = addDataToDHT(job, keyfilePath, taskValues, filePartCounter, futurePutValues, futurePutKeys);
					}
				}
			} catch (IOException x) {
				System.err.format("IOException: %s%n", x);
			}

			if (taskDataComposer.currentValues() != null) {
				logger.info("Adding last data set: " + taskDataComposer.currentValues());
				// filePartCounter = addDataToDHT(job, keyfilePath, taskDataComposer.currentValues(), filePartCounter, futurePutValues,
				// futurePutKeys);
				taskDataComposer.reset();
			}
		}
		// Futures.whenAllSuccess(futurePutValues).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {
		//
		// @Override
		// public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
		// if (future.isSuccess()) {
		// Futures.whenAllSuccess(futurePutKeys).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {
		//
		// @Override
		// public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
		// if (future.isSuccess()) {
		// dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).addListener(new BaseFutureAdapter<FuturePut>() {
		//
		// @Override
		// public void operationComplete(FuturePut future) throws Exception {
		// logger.info("Broadcast initial complete procedure");
		// logger.info("resOutputDomain:" + job.currentProcedure().resultOutputDomain());
		// logger.info("dataInputDomain:" + inputDomain);
		// JobProcedureDomain inputDomain = job.currentProcedure().dataInputDomain()
		// .nrOfFinishedTasks(futurePutValues.size());
		// CompletedBCMessage msg = CompletedBCMessage
		// .createCompletedProcedureBCMessage(job.currentProcedure().resultOutputDomain(), inputDomain);
		// dhtConnectionProvider.broadcastCompletion(msg);
		// }
		//
		// });
		// } else {
		// logger.info("Could not add keys for " + job.fileInputFolderPath());
		// }
		// }
		// });
		// } else {
		// logger.info("Could not add values for " + job.fileInputFolderPath());
		// }
		// }
		// });

	}

	private int addDataToDHT(final Job job, String keyfilePath, String taskValue, int filePartCounter, List<FuturePut> futurePutValues,
			List<FuturePut> futurePutKeys) {
		String fileName = keyfilePath.replace("/", "\\");
		String taskKey = fileName + "_" + filePartCounter++;

		futurePutValues.add(dhtConnectionProvider.add(taskKey, taskValue, job.currentProcedure().resultOutputDomain().toString(), true)
				.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							// Add <PROCEDURE_KEYS, key, jobProcedureDomain> to collect all domains for all keys
							futurePutKeys
									.add(dhtConnectionProvider
											.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, taskKey,
													job.currentProcedure().resultOutputDomain().toString(), false)
											.addListener(new BaseFutureAdapter<FuturePut>() {

								@Override
								public void operationComplete(FuturePut future) throws Exception {
									if (future.isSuccess()) {
										logger.info("Successfully added data.");
									} else {
										logger.warn(future.failedReason());
									}
								}

							}));
						} else {
							logger.warn(future.failedReason());
						}
					}
				}));

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

	public void finishedJob(JobProcedureDomain resultDomain) {
		this.resultDomain = resultDomain;
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
				}
			}
		});
	}

	private void write(String toAppend, boolean isKey) {
		if (isKey) {
			toAppend = "\n" + toAppend;
		}
		String values = taskDataComposer.append(toAppend);
		if (values != null) {
			Path file = Paths.get(outputFolder);
			Charset charset = Charset.forName(taskDataComposer.fileEncoding());
			try (BufferedWriter writer = Files.newBufferedWriter(file, charset)) {
				writer.write(values);
				writer.flush();
			} catch (IOException x) {
				System.err.format("IOException: %s%n", x);
			}
		}

	}
}
