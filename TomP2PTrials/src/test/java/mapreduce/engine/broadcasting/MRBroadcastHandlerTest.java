package mapreduce.engine.broadcasting;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;

public class MRBroadcastHandlerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() throws Exception {

		SortedMap<Job, PriorityBlockingQueue<IBCMessage>> jobs = SyncedCollectionProvider.syncedTreeMap();
		IDHTConnectionProvider dht = TestUtils.getTestConnectionProvider(8484, 1).owner("TEST1").jobQueues(jobs);
		Job job = Job.create("ME").addSucceedingProcedure(WordCountMapper.create()).addSucceedingProcedure(WordCountReducer.create());

 		JobProcedureDomain outputDomain = JobProcedureDomain.create(job.id(), "ME", "WORDCOUNTMAPPER", 1);
		JobProcedureDomain inputDomain = JobProcedureDomain.create(job.id(), "ME", "WORDCOUNTREDUCER", 2);

		// Before job was put into dht
		CompletedBCMessage msg = CompletedBCMessage.createCompletedProcedureBCMessage(outputDomain, inputDomain);
		dht.broadcastCompletion(msg);
		Thread.sleep(1000);
		assertEquals(0, jobs.size());

		dht.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();

		// after job was put into dht
		dht.broadcastCompletion(msg);
		Thread.sleep(1000);
		assertEquals(1, jobs.size());
		System.err.println(jobs.get(jobs.firstKey()));
	}

}
