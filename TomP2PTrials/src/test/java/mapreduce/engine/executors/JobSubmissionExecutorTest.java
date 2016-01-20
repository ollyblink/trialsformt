package mapreduce.engine.executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.jobs.Job;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class JobSubmissionExecutorTest {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionExecutorTest.class);

	private static JobSubmissionExecutor sExecutor = JobSubmissionExecutor.create();

	@Ignore
	public void testSubmission() throws IOException {
		String fileInputFolderPath = System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/testFiles";

		JobSubmissionBroadcastHandler sBCHandler = Mockito.mock(JobSubmissionBroadcastHandler.class);
		JobSubmissionMessageConsumer sMsgConsumer = Mockito.mock(JobSubmissionMessageConsumer.class);

		Mockito.when(sMsgConsumer.executor()).thenReturn(sExecutor);
		Mockito.when(sBCHandler.executorId()).thenReturn(sExecutor.id());
		Mockito.when(sBCHandler.messageConsumer()).thenReturn(sMsgConsumer);

		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(5001, 1)
				.broadcastHandler(sBCHandler);
		sExecutor.dhtConnectionProvider(dhtConnectionProvider);

		Job job = Job.create(sExecutor.id()).fileInputFolderPath(fileInputFolderPath)
				.maxFileSize(FileSize.TWO_KILO_BYTES);
		sExecutor.submit(job);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		ArrayList<FutureGet> keysFutures = new ArrayList<>();
		ArrayList<FutureGet> valueFutures = new ArrayList<>();
		ListMultimap<String, Object> vals = ArrayListMultimap.create();
		String dataLocationDomain = job.currentProcedure().resultOutputDomain().toString();
		logger.info("Data location domain " + dataLocationDomain);
		keysFutures.add(
				dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, dataLocationDomain)
						.addListener(new BaseFutureAdapter<FutureGet>() {

							@Override
							public void operationComplete(FutureGet future) throws Exception {
								if (future.isSuccess()) {
									for (Number640 n : future.dataMap().keySet()) {
										String key = (String) future.dataMap().get(n).object();
										logger.info("Found key " + key);
										valueFutures.add(dhtConnectionProvider.getAll(key, dataLocationDomain)
												.addListener(new BaseFutureAdapter<FutureGet>() {

											@Override
											public void operationComplete(FutureGet future) throws Exception {
												if (future.isSuccess()) {
													for (Number640 n : future.dataMap().keySet()) {
														Object value = ((Value) future.dataMap().get(n)
																.object()).value();
														vals.put(key, value);
														logger.info(
																"Found value for key " + key + ": " + value);
														// C:\Users\Oliver\git\trialsformt3\TomP2PTrials/src/test/java/mapreduce/manager/testFiles/testfile.txt_1
													}
												} else {
													logger.warn("Failed: " + future.failedReason());
												}
											}
										}));
									}
								} else {
									logger.warn("Failed: " + future.failedReason());
								}
							}
						}));
		// C:\Users\Oliver\git\trialsformt3\TomP2PTrials\src\test\java\mapreduce\manager\testFiles\testfile.txt_0
		final List<Boolean> finished = SyncedCollectionProvider.syncedArrayList();
		finished.add(false);
		Futures.whenAllSuccess(keysFutures).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

			@Override
			public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {

				if (future.isSuccess()) {

					logger.info("Retrieved all values for keys");
					Futures.whenAllSuccess(valueFutures)
							.addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

						@Override
						public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
							logger.info("Retrieved all values");

							if (future.isSuccess()) {
								ListMultimap<String, Object> toCheck = getToCheck(fileInputFolderPath,
										job.maxFileSize());

								for (String key : toCheck.keySet()) {
									List<Object> values = toCheck.get(key);
									logger.info("Retrieved all values for key " + key + ":" + values);
									assertEquals(true, vals.containsKey(key));
									for (Object o : values) {
										assertEquals(true, vals.containsValue(o));
									}
									logger.info(key + ": " + vals);
								}
								finished.set(0, true);
							} else {
								logger.info("No success on retrieving all values");
							}

						}

					});
				} else {
					logger.info("No success on retrieving all keys");
				}

			}

		});

		int waitCounter = 0;
		try {
			while (!finished.get(0) && waitCounter++ < 5) {
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private ListMultimap<String, Object> getToCheck(String fileInputFolderPath, FileSize maxFileSize) {
		ListMultimap<String, Object> data = ArrayListMultimap.create();
		// MaxFileSizeTaskDataComposer taskDataComposer =
		// MaxFileSizeTaskDataComposer.create().maxFileSize(maxFileSize);
		//
		// String keyFilePath = fileInputFolderPath +
		// "/testfile.txt";
		// Path path = Paths.get(keyFilePath);
		// Charset charset =
		// Charset.forName(taskDataComposer.fileEncoding());
		//
		// int filePartCounter = 0;
		// try (BufferedReader reader =
		// Files.newBufferedReader(path, charset)) {
		// String line = null;
		// while ((line = reader.readLine()) != null) {
		// String taskValues = taskDataComposer.append(line);
		// if (taskValues != null) {
		// String fileName = keyFilePath.replace("/", "\\");
		// String taskKey = fileName + "_" + filePartCounter++;
		// data.put(taskKey, taskValues);
		// }
		// }
		// } catch (IOException x) {
		// System.err.format("IOException: %s%n", x);
		// }
		// if (taskDataComposer.currentValues() != null) {
		// String fileName = keyFilePath.replace("/", "\\");
		// String taskKey = fileName + "_" + filePartCounter++;
		// data.put(taskKey, taskDataComposer.currentValues());
		// }
		return data;
	}

	@Test
	public void testJobIsRetrievedAndMarkAsRetrievedAndJobAndSubmittedJob() throws Exception {
		// ================================================================================================
		// Testing 4 simple methods: jobIsRetrieved, job, submittedJob, markAsRetrieved
		// ================================================================================================
		Field submittedJobsField = JobSubmissionExecutor.class.getDeclaredField("submittedJobs");
		submittedJobsField.setAccessible(true);
		Set<Job> submittedJobs = (Set<Job>) submittedJobsField.get(sExecutor);
		Job job = Job.create(sExecutor.id());
		Job job2 = Job.create(sExecutor.id());
		assertEquals(false, sExecutor.submittedJob(job));
		assertEquals(false, sExecutor.submittedJob(job2));
		assertEquals(false, sExecutor.jobIsRetrieved(job));
		assertEquals(false, sExecutor.jobIsRetrieved(job2));
		assertEquals(null, sExecutor.job(job.id()));
		assertEquals(null, sExecutor.job(job2.id()));
		submittedJobs.add(job);
		submittedJobs.add(job2);
		assertEquals(true, sExecutor.submittedJob(job));
		assertEquals(true, sExecutor.submittedJob(job2));
		assertEquals(false, sExecutor.jobIsRetrieved(sExecutor.job(job.id())));
		assertEquals(false, sExecutor.jobIsRetrieved(sExecutor.job(job2.id())));
		sExecutor.markAsRetrieved(job.id());
		assertEquals(true, sExecutor.jobIsRetrieved(sExecutor.job(job.id())));
		assertEquals(false, sExecutor.jobIsRetrieved(sExecutor.job(job2.id())));
		sExecutor.markAsRetrieved(job2.id());
		assertEquals(true, sExecutor.jobIsRetrieved(sExecutor.job(job.id())));
		assertEquals(true, sExecutor.jobIsRetrieved(sExecutor.job(job2.id())));
	}

	@Test
	public void testSubmitInternally() throws Exception {
		Method write = JobSubmissionExecutor.class.getDeclaredMethod("submitInternally");
		fail();
	}

	@Test
	public void testReadFile() throws Exception {
		Method write = JobSubmissionExecutor.class.getDeclaredMethod("readFile");
		fail();
	}

	@Test
	public void testSubmit() throws Exception {
		fail();
	}

	@Test
	public void testRetrieveAndStoreDataOfFinishedJob() {
		fail();
	}

	@Test
	public void testWriteFlush() throws Exception {
		Method write = JobSubmissionExecutor.class.getDeclaredMethod("write", String.class);
		write.setAccessible(true);
		Method flush = JobSubmissionExecutor.class.getDeclaredMethod("flush");
		flush.setAccessible(true);
		String outputFolder = System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/testOutputFiles/";
		sExecutor.outputFileSize(FileSize.SIXTEEN_BYTES.value()).outputFolder(outputFolder);

		write.invoke(sExecutor, "hello\t20");
		write.invoke(sExecutor, "world\t15");
		write.invoke(sExecutor, "this\t10");
		write.invoke(sExecutor, "is\t5");
		write.invoke(sExecutor, "a\t10");
		write.invoke(sExecutor, "test\t11");
		flush.invoke(sExecutor);
		List<String> pathVisitor = new ArrayList<>();
		FileUtils.INSTANCE.getFiles(new File(outputFolder), pathVisitor);

		assertEquals(3, pathVisitor.size());
	}

	@Test
	public void testEstimatedNrOfFiles() throws Exception {
		Method estimatedNrOfFilesMethod = JobSubmissionExecutor.class.getDeclaredMethod("estimatedNrOfFiles",
				List.class, Long.class);
		estimatedNrOfFilesMethod.setAccessible(true);
		List<String> keyFilePaths = new ArrayList<>();
		keyFilePaths.add(
				System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/testFiles/testfile2.txt");
		int nrFiles = (int) estimatedNrOfFilesMethod.invoke(sExecutor, keyFilePaths, 20l);
		assertEquals(3, nrFiles);
	}

	@Test
	public void testFilePaths() throws Exception {
		String fileInputFolderPath = System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/testFiles";
		Method filePaths = JobSubmissionExecutor.class.getDeclaredMethod("filePaths", String.class);
		filePaths.setAccessible(true);
		List<String> keyFilePaths = (List<String>) filePaths.invoke(sExecutor, fileInputFolderPath);
		assertEquals(1, keyFilePaths.size());
		assertEquals("testfile.txt", new File(keyFilePaths.get(0)).getName());
	}
}
