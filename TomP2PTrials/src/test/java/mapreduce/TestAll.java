package mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.client.MRJobSubmitter;
import mapreduce.execution.computation.context.NullContext;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.server.MRJobExecutor;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.utils.FileUtils;
import net.tomp2p.peers.PeerAddress;

public class TestAll {

	private static final Random RND = new Random(42l);
	private static ArrayList<MRJobExecutor> executors;
	private static Job job;
	private static MRJobSubmitter submitter;
	private static ExecutorService server;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;

		DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newDHTConnectionProvider();
		int id = 1;
		if (id != 1) {
			dhtConnectionProvider.bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort);
		} else {
			dhtConnectionProvider.port(bootstrapPort);
		}

		MRJobExecutor executor = MRJobExecutor.newJobExecutor(dhtConnectionProvider).context(NullContext.newNullContext());

		executor.start();
		submitter = MRJobSubmitter
				.newMapReduceJobSubmitter(DHTConnectionProvider.newDHTConnectionProvider().bootstrapIP(bootstrapIP).bootstrapPort(bootstrapPort));


		// String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/firstdesignidea/execution/datasplitting/testfile";
		String inputPath = "/home/ozihler/Desktop/input_small";
		if (new File(inputPath + "/tmp").exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));
		}

		long megaByte = 1024 * 1024;

		int maxNumberOfFinishedPeers = 3;
		job = Job.newJob().nextProcedure(new WordCountMapper(), null).maxNrOfFinishedPeers(maxNumberOfFinishedPeers).inputPath(inputPath)
				.maxFileSize(megaByte);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// submitter.shutdown();
		// for (MRJobExecutor e : executors) {
		// e.shutdown();
		// }
		// server.shutdown();
		// while (!server.isTerminated()) {
		// Thread.sleep(1000);
		// }
	}

	@Test
	public void test() throws InterruptedException {

		submitter.submit(job);
		Thread.sleep(10000);
		for (MRJobExecutor e : executors) {
			Job job = e.getJob();
			System.out.println("JOB: " + job.id());
			System.out.println("Procedure: " + job.nextProcedure());
			BlockingQueue<Task> tasksFor = job.tasksFor(job.nextProcedure());
			for (Task t : tasksFor) {
				System.out.println("Task: " + t.id());
				Set<PeerAddress> assignedPeers = t.allAssignedPeers();
				for (PeerAddress p : assignedPeers) {
					System.out.println(p.inetAddress() + "/" + p.tcpPort() + ": " + t.statiForPeer(p));
				}
				System.out.println();
			}
			System.out.println();
		}

	}

}
