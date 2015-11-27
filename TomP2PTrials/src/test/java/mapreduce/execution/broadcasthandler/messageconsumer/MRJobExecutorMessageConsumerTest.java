package mapreduce.execution.broadcasthandler.messageconsumer;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.datasplitting.ITaskSplitter;
import mapreduce.execution.datasplitting.MaxFileSizeTaskSplitter;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.utils.FileUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutorMessageConsumerTest {

	private static MRJobExecutorMessageConsumer m;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BlockingQueue<Job> jobs = new LinkedBlockingQueue<Job>();
		m = MRJobExecutorMessageConsumer.newMRJobExecutorMessageConsumer(jobs);

		// m.updateTask(String jobId, String taskId, PeerAddress peerAddress, JobStatus currentStatus);
		// public void handleFinishedJob(String jobId, String jobSubmitterId);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testAll() {

		String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/datasplitting/testfile";
		if (new File(inputPath + "/tmp").exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));
		}

		long megaByte = 1024 * 1024;

		int maxNumberOfFinishedPeers = 3;
		Job job = Job.newJob("TEST").nextProcedure(new WordCountMapper()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers).inputPath(inputPath)
				.maxFileSize(megaByte);

		Job jobCopy = Job.newJob("TEST").nextProcedure(new WordCountMapper()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers)
				.inputPath(inputPath).maxFileSize(megaByte);

		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newMaxFileSizeTaskSplitter();
		splitter.split(job);
		splitter.split(jobCopy);
		BlockingQueue<Task> tasks = job.tasks(0);

		for (Task task : tasks) {
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(1)), JobStatus.EXECUTING_TASK);
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(1)), JobStatus.FINISHED_TASK);
		}
		m.addJob(job);

		String jobId = job.id();
		BlockingQueue<Task> tasks2 = jobCopy.tasks(0);
		for (Task task : tasks2) {
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(1)), JobStatus.EXECUTING_TASK);
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(1)), JobStatus.FINISHED_TASK);
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(1)), JobStatus.EXECUTING_TASK);
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(1)), JobStatus.FINISHED_TASK);
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(2)), JobStatus.EXECUTING_TASK);
			
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(2)), JobStatus.FINISHED_TASK);
			m.updateTask(jobId, task.id(), new PeerAddress(new Number160(2)), JobStatus.FINISHED_TASK);
			
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(2)), JobStatus.EXECUTING_TASK);
			m.updateTask(jobId, task.id(), new PeerAddress(new Number160(2)), JobStatus.EXECUTING_TASK);
			
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(2)), JobStatus.FINISHED_TASK);
			
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(3)), JobStatus.EXECUTING_TASK);
			m.updateTask(jobId, task.id(), new PeerAddress(new Number160(3)), JobStatus.EXECUTING_TASK);
			
			task.updateExecutingPeerStatus(new PeerAddress(new Number160(3)), JobStatus.FINISHED_TASK);
			m.updateTask(jobId, task.id(), new PeerAddress(new Number160(3)), JobStatus.FINISHED_TASK);
		}

		assertTrue(m.jobs().peek().tasks(0).peek().allAssignedPeers().size() == 3);
		assertTrue(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(1))).size() == 1);
		assertTrue(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(2))).size() == 1);
		assertTrue(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(3))).size() == 1);
		assertTrue(new ArrayList<JobStatus>(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(1)))).get(0).equals(JobStatus.FINISHED_TASK));
		assertTrue(new ArrayList<JobStatus>(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(2)))).get(0).equals(JobStatus.EXECUTING_TASK));
		assertTrue(new ArrayList<JobStatus>(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(3)))).get(0).equals(JobStatus.FINISHED_TASK));

		m.handleFinishedTasks(jobId, tasks2);

		for (Task task : m.jobs().peek().tasks(0)) {
			assertTrue(task.allAssignedPeers().size() == 3);
			System.err.println(task.allAssignedPeers());
			int number = 0;
			for (int i = 1; i <= 3; ++i) {
				if (i == 1 || i == 2) {
					number = 2;
				} else if (i == 3)
					number = 1;
				assertTrue(task.statiForPeer(new PeerAddress(new Number160(i))).size() == number);
				for (JobStatus s : task.statiForPeer(new PeerAddress(new Number160(i)))) {
					assertTrue(s.equals(JobStatus.FINISHED_TASK));
					System.err.println(s);
				}
			}
		}
	}

}
