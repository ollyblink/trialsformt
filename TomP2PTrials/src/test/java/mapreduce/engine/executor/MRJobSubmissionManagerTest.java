package mapreduce.engine.executor;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.tasks.taskdatacomposing.MaxFileSizeTaskDataComposer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class MRJobSubmissionManagerTest {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmissionManagerTest.class);

	private static JobSubmissionExecutor jobSubmissionManager;

	@Test
	public void test() throws IOException {
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/testFiles";

		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(5001, 1);
		jobSubmissionManager = JobSubmissionExecutor.create(dhtConnectionProvider);

		Job job = Job.create(jobSubmissionManager.id()).fileInputFolderPath(fileInputFolderPath).maxFileSize(FileSize.TWO_KILO_BYTES);
		jobSubmissionManager.submit(job);

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
		keysFutures.add(dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, dataLocationDomain)
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							for (Number640 n : future.dataMap().keySet()) {
								String key = (String) future.dataMap().get(n).object();
								logger.info("Found key " + key);
								valueFutures
										.add(dhtConnectionProvider.getAll(key, dataLocationDomain).addListener(new BaseFutureAdapter<FutureGet>() {

									@Override
									public void operationComplete(FutureGet future) throws Exception {
										if (future.isSuccess()) {
											for (Number640 n : future.dataMap().keySet()) {
												Object value = ((Value) future.dataMap().get(n).object()).value();
												vals.put(key, value);
												logger.info("Found value for key " + key + ": " + value);
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
					Futures.whenAllSuccess(valueFutures).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

						@Override
						public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
							logger.info("Retrieved all values");

							if (future.isSuccess()) {
								ListMultimap<String, Object> toCheck = getToCheck(fileInputFolderPath, job.maxFileSize());

								for (String key : toCheck.keySet()) {
									List<Object> values = toCheck.get(key);
									logger.info("Retrieved all values for key " + key + ":" + values);
									assertEquals(true, vals.containsKey(key));
									for (Object o : values) {
										assertEquals(true, vals.containsValue(o));
									}
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
		MaxFileSizeTaskDataComposer taskDataComposer = MaxFileSizeTaskDataComposer.create().maxFileSize(maxFileSize);

		String keyFilePath = fileInputFolderPath + "/testfile.txt";
		Path path = Paths.get(keyFilePath);
		Charset charset = Charset.forName(taskDataComposer.fileEncoding());

		int filePartCounter = 0;
		try (BufferedReader reader = Files.newBufferedReader(path, charset)) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				String taskValues = taskDataComposer.append(line);
				if (taskValues != null) {
					String fileName = keyFilePath.replace("/", "\\");
					String taskKey = fileName + "_" + filePartCounter++;
					data.put(taskKey, taskValues);
				}
			}
		} catch (IOException x) {
			System.err.format("IOException: %s%n", x);
		}
		if (taskDataComposer.currentValues() != null) {
			String fileName = keyFilePath.replace("/", "\\");
			String taskKey = fileName + "_" + filePartCounter++;
			data.put(taskKey, taskDataComposer.currentValues());
		}
		return data;
	}

}
