package mapreduce.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MRJobExecutorTest {

	private static MRJobExecutionManager executor;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/*
	 *  
	 * 
	 */

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() {

		executor.start();
	}

}
