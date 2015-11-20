package firstdesignidea.server;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import firstdesignidea.client.MRJobSubmitter;
import firstdesignidea.execution.broadcasthandler.MRBroadcastHandler;
import firstdesignidea.execution.computation.standardprocedures.WordCountMapper;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.storage.DHTConnectionProvider;
import firstdesignidea.utils.FileUtils;

public class MRJobExecutorTest {

	private static final Random RND = new Random(42l);
	private static ArrayList<MRJobExecutor> executors;
	private static MRJobSubmitter submitter;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;
		int maxNrOfFinishedPeers = 3;

		executors = new ArrayList<MRJobExecutor>();
		for (int id = 1; id < 10; ++id) {
			DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newDHTConnectionProvider()
					.broadcastDistributor(new MRBroadcastHandler());

			if (id != 1) {
				dhtConnectionProvider.bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort);
			} else {
				dhtConnectionProvider.port(bootstrapPort);
			}

			executors.add(MRJobExecutor.newJobExecutor(dhtConnectionProvider).maxNrOfFinishedPeers(maxNrOfFinishedPeers));
		}
		submitter = MRJobSubmitter.newMapReduceJobSubmitter().dhtConnectionProvider(DHTConnectionProvider.newDHTConnectionProvider()
				.broadcastDistributor(new MRBroadcastHandler()).bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort));

		for (MRJobExecutor e : executors) {
			e.start();
		}

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() throws InterruptedException {
		String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/firstdesignidea/execution/datasplitting/testfile";
		FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));

		long megaByte = 1024 * 1024;
		 
		Job job = Job.newJob().procedures(new WordCountMapper()).inputPath(inputPath).maxFileSize(megaByte);

		submitter.submit(job);
		Thread.sleep(Long.MAX_VALUE);
	}

}
