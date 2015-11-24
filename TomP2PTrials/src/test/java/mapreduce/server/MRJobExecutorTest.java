package mapreduce.server;

import static org.junit.Assert.*;

import java.util.concurrent.BlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.execution.jobtask.Job;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;

public class MRJobExecutorTest {

	private static MRJobExecutor executor;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		IDHTConnectionProvider dhtConnectionProvider = Mockito.mock(IDHTConnectionProvider.class);
		 
		executor = MRJobExecutor.newJobExecutor(dhtConnectionProvider);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() {
//		executor.start(job);
 	}

}
