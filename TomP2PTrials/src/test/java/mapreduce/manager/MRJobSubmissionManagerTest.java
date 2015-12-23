package mapreduce.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.computation.context.PrintContext;
import mapreduce.execution.computation.context.PseudoStorageContext;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.computation.standardprocedures.WordCountReducer;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.DHTUtils;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number640;
import static org.junit.Assert.*;

public class MRJobSubmissionManagerTest {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmissionManagerTest.class);

	private static MRJobSubmissionManager jobSubmissionManager;

	@Test
	public void test() throws UnsupportedEncodingException {

		String bootstrapIP = "192.168.43.65";
		int bootstrapPort = 4000;
		DHTUtils dhtUtils = DHTUtils.newInstance(bootstrapIP, bootstrapPort);
		IDHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newInstance(dhtUtils);
		jobSubmissionManager = MRJobSubmissionManager.newInstance(dhtConnectionProvider);

		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/manager/testFiles";
		System.err.println(fileInputFolderPath + " : " + new File(fileInputFolderPath).exists());
		Job job = Job.create(jobSubmissionManager.id()).fileInputFolderPath(fileInputFolderPath).nextProcedure(WordCountMapper.newInstance());

		final ListMultimap<Object, Object> toCheck = getToCheck(fileInputFolderPath);

		jobSubmissionManager.submit(job);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) { 
			e.printStackTrace();
		}
		
		final List<Boolean> finished = new ArrayList<>();
		finished.add(false);
		dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_KEYS, DomainProvider.INSTANCE.jobProcedureDomain(job))
				.addListener(new BaseFutureListener<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							try {
								if (future.dataMap() != null) {
									for (Number640 n : future.dataMap().keySet()) {
										String key = (String) future.dataMap().get(n).object();
										dhtConnectionProvider.getAll(key, DomainProvider.INSTANCE.jobProcedureDomain(job))
												.addListener(new BaseFutureListener<FutureGet>() {

											@Override
											public void operationComplete(FutureGet future) throws Exception {
												if (future.isSuccess()) {
													try {
														if (future.dataMap() != null) {
															for (Number640 n : future.dataMap().keySet()) {
																String taskExecutorDomain = (String) future.dataMap().get(n).object();
																dhtConnectionProvider.getAll(key, taskExecutorDomain)
																		.addListener(new BaseFutureListener<FutureGet>() {

																	@Override
																	public void operationComplete(FutureGet future) throws Exception {
																		if (future.dataMap() != null) {
																			List<Object> values = new ArrayList<>();
																			for (Number640 n : future.dataMap().keySet()) {
																				Object value = ((Value) future.dataMap().get(n).object()).value();
																				values.add(value);
																			}
																			PseudoStorageContext context = (PseudoStorageContext) PseudoStorageContext
																					.newInstance().combiner(WordCountReducer.newInstance())
																					.task(Task.newInstance(key, job.id()));
																			job.currentProcedure().procedure().process(key, values, context);
																			ListMultimap<Object, Object> storage = context.storage();
																			for (Object key : storage.keySet()) {
																				assertEquals(true, toCheck.containsKey(key));
																				assertEquals(true, toCheck.get(key).get(0).equals(storage.get(key).get(0)));
																				finished.set(0, true);
																			}
																		}
																	}

																	@Override
																	public void exceptionCaught(Throwable t) throws Exception {
																		logger.warn("Exception", t);
																	}
																});
															}
														} else {

															logger.warn("Failed: " + future.failedReason());
														}

													} catch (Exception e) {

														logger.warn("Exception", e);
													}
												}
											}

											@Override
											public void exceptionCaught(Throwable t) throws Exception {
												logger.warn("Exception", t);
											}
										});
									}

								}
							} catch (IOException e) {
								logger.warn("Exception", e);

							}
						} else {
							logger.warn("Failed: " + future.failedReason());
						}

					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						logger.warn("Exception", t);
					}
				});

		try {
			while(!finished.get(0)){
				Thread.sleep(10);
			}
		} catch (InterruptedException e) { 
			e.printStackTrace();
		}
		// Multimap<Task, Comparable> keysForEachTask = splitter.keysForEachTask();
		// for (Task task : keysForEachTask.keySet()) {
		// Multimap<Object, Object> taskData = ArrayListMultimap.create();
		// dhtConnectionProvider.getTaskData(task, task.initialDataLocation(), taskData);
		//
		// for (Object key : taskData.keySet()) {
		// // System.err.println(job.maxFileSize() + " " + new ArrayList<Object>(taskData.get(key)).get(0).toString().getBytes("UTF-8").length);
		// assertTrue(keysForEachTask.containsValue(key));
		// assertTrue(job.maxFileSize() >= new ArrayList<Object>(taskData.get(key)).get(0).toString().getBytes("UTF-8").length);
		// }
		// }
		// jobSubmissionManager.shutdown();
	}

	private ListMultimap<Object, Object> getToCheck(String fileInputFolderPath) {
		PseudoStorageContext storage = (PseudoStorageContext) PseudoStorageContext.newInstance().combiner(WordCountReducer.newInstance());

		try {
			System.err.println(new File(fileInputFolderPath + "/testfile.txt").exists());
			BufferedReader reader = new BufferedReader(new FileReader(new File(fileInputFolderPath + "/testfile.txt")));
			String line = null;

			while ((line = reader.readLine()) != null) {
				List<Object> values = new ArrayList<>();
				values.add(line);
				System.err.println(line);
				WordCountMapper.newInstance().process(fileInputFolderPath + "/testfile.txt", values, storage);
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return storage.storage();
	}

}
