package mapreduce.manager;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.computation.context.PseudoStorageContext;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.computation.standardprocedures.WordCountReducer;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class MRJobSubmissionManagerTest {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmissionManagerTest.class);

	private static MRJobSubmissionManager jobSubmissionManager;

	@Test
	public void test() throws IOException {

		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/manager/testFiles";

		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(5001);
		jobSubmissionManager = MRJobSubmissionManager.newInstance(dhtConnectionProvider);

		Job job = Job.create(jobSubmissionManager.id()).fileInputFolderPath(fileInputFolderPath).maxFileSize(FileSize.TWO_KILO_BYTES)
				.addSubsequentProcedure(WordCountMapper.create());
		jobSubmissionManager.submit(job);

		final ListMultimap<Object, Object> toCheck = getToCheck(fileInputFolderPath);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.err.println("HERE");
		ProcedureInformation pI = job.currentProcedure();
		String jobProcedureDomain = pI.jobProcedureDomain();
		
		ArrayList<FutureGet> keysFutures = new ArrayList<>();
		ArrayList<FutureGet> taskExecutorDomainFutures = new ArrayList<>();
		ArrayList<FutureGet> valueFutures = new ArrayList<>();
		ListMultimap<String, Object> vals = ArrayListMultimap.create();
		keysFutures
				.add(dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_KEYS, jobProcedureDomain).addListener(new BaseFutureListener<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							try {
								if (future.dataMap() != null) {
									for (Number640 n : future.dataMap().keySet()) {
										String key = (String) future.dataMap().get(n).object();
										// System.err.println("Key: " + key);
										taskExecutorDomainFutures.add(dhtConnectionProvider.getAll(key, jobProcedureDomain)
												.addListener(new BaseFutureListener<FutureGet>() {

											@Override
											public void operationComplete(FutureGet future) throws Exception {
												if (future.isSuccess()) {
													try {
														if (future.dataMap() != null) {
															for (Number640 n : future.dataMap().keySet()) {
																String taskExecutorDomain = (String) future.dataMap().get(n).object();
																String taskExecutorDomainCombination = jobProcedureDomain + "_" + taskExecutorDomain;

																// System.err.println("taskExecutorDomain: " + taskExecutorDomainCombination);
																valueFutures.add(dhtConnectionProvider.getAll(key, taskExecutorDomainCombination)
																		.addListener(new BaseFutureListener<FutureGet>() {

																	@Override
																	public void operationComplete(FutureGet future) throws Exception {
																		if (future.dataMap() != null) {
																			for (Number640 n : future.dataMap().keySet()) {
																				Object value = ((Value) future.dataMap().get(n).object()).value();
																				vals.put(key, value);
																				// System.err.println("Value: <"+key+", "+value+">");
																			}

																		}
																	}

																	@Override
																	public void exceptionCaught(Throwable t) throws Exception {
																		logger.warn("Exception", t);
																	}
																}));
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
										}));

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
				}));
		final List<Boolean> finished = new ArrayList<>();
		finished.add(false);
		Futures.whenAllSuccess(keysFutures).addListener(new BaseFutureListener<FutureDone<FutureGet[]>>() {

			@Override
			public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {

				if (future.isSuccess()) {
					Futures.whenAllSuccess(taskExecutorDomainFutures).addListener(new BaseFutureListener<FutureDone<FutureGet[]>>() {

						@Override
						public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {

							if (future.isSuccess()) {
								Futures.whenAllSuccess(valueFutures).addListener(new BaseFutureListener<FutureDone<FutureGet[]>>() {

									@Override
									public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {

										if (future.isSuccess()) {

											PseudoStorageContext context = (PseudoStorageContext) PseudoStorageContext.newInstance()
													.combiner(WordCountReducer.newInstance());
											for (String key : vals.keySet())

											{
												context.task(Task.create(key, job.id()));
												job.currentProcedure().procedure().process(key, vals.get(key), context);

											}

											ListMultimap<Object, Object> storage = context.storage();
											for (Object key : storage.keySet())

											{
												System.err.println("DHT: " + key + " " + storage.get(key));
												System.err.println("To check: " + key + " " + toCheck.get(key).get(0));
												assertEquals(true, toCheck.containsKey(key));
												assertEquals(true, toCheck.get(key).get(0).equals(storage.get(key).get(0)));
												finished.set(0, true);
											}
										}

									}

									@Override
									public void exceptionCaught(Throwable t) throws Exception {
										// TODO Auto-generated method stub

									}

								});
							}

						}

						@Override
						public void exceptionCaught(Throwable t) throws Exception {
							// TODO Auto-generated method stub

						}

					});
				}

			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				// TODO Auto-generated method stub

			}

		});

		try {
			while (!finished.get(0)) {
				Thread.sleep(10);
			}
		} catch (

		InterruptedException e)

		{
			e.printStackTrace();
		}

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
				WordCountMapper.create().process(fileInputFolderPath + "/testfile.txt", values, storage);
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return storage.storage();
	}

}
