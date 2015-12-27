package mapreduce.manager;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.computation.standardprocedures.WordCountReducer;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;

public class MRJobExecutionManagerTest {

	private static MRJobExecutionManager jobExecutor;
	private static IDHTConnectionProvider dhtConnectionProvider;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		// IDHTConnectionProvider con = TestUtils.getTestConnectionProvider(4000);

		// String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/manager/testFiles";

		dhtConnectionProvider = TestUtils.getTestConnectionProvider(4000);
		jobExecutor = MRJobExecutionManager.newInstance(dhtConnectionProvider);
		jobExecutor.start();

		// MRJobSubmissionManager jobSubmissionManager = MRJobSubmissionManager.newInstance(dhtConnectionProvider);

		// jobSubmissionManager.submit(job);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() throws Exception {
		Job job = Job.create("TEST").addSubsequentProcedure(WordCountMapper.newInstance())
		// .addSubsequentProcedure(WordCountReducer.newInstance())
		;
		String key = "file1";
		String value = "hello world hello world hello world world world world";
		Task task = Task.newInstance(key, job.id());
		String taskExecutorDomain = DomainProvider.INSTANCE.executorTaskDomain(task, Tuple.create(dhtConnectionProvider.owner(), 0));

		ProcedureInformation pI = job.currentProcedure();
		String jobProcedureDomain = pI.jobProcedureDomain();
		String taskExecutorDomainConcatenation = DomainProvider.INSTANCE.concatenation(pI, task, Tuple.create(dhtConnectionProvider.owner(), 0));
		List<FuturePut> futurePutData = SyncedCollectionProvider.syncedArrayList();
		List<FuturePut> futurePutTEDomain = SyncedCollectionProvider.syncedArrayList();
		List<FuturePut> futurePutProcKey = SyncedCollectionProvider.syncedArrayList();
		futurePutData.add(
				dhtConnectionProvider.add(task.id(), value, taskExecutorDomainConcatenation, true).addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							futurePutTEDomain.add(dhtConnectionProvider.add(task.id(), taskExecutorDomain, jobProcedureDomain, false)
									.addListener(new BaseFutureAdapter<FuturePut>() {

								@Override
								public void operationComplete(FuturePut future) throws Exception {
									if (future.isSuccess()) {
										futurePutProcKey
												.add(dhtConnectionProvider.add(DomainProvider.PROCEDURE_KEYS, task.id(), jobProcedureDomain, false));
									}
								}
							}));
						}
					}
				}));

		Futures.whenAllSuccess(futurePutData).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					Futures.whenAllSuccess(futurePutTEDomain).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

						@Override
						public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
							if (future.isSuccess()) {
								Futures.whenAllSuccess(futurePutProcKey).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

									@Override
									public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
										if (future.isSuccess()) {
											jobExecutor.jobs().add(job);
											jobExecutor.execute(job);
										}
									}
								});

							}
						}
					});

				}
			}
		});
		Thread.sleep(100000);
	}

}
