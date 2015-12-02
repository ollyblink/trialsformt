package mapreduce.execution.broadcasthandler.messageconsumer;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.datasplitting.ITaskSplitter;
import mapreduce.execution.datasplitting.MaxFileSizeTaskSplitter;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobExecutorMessageConsumer;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCStatusType;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.utils.FileUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutorMessageConsumerTest {

	private static MRJobExecutorMessageConsumer m;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BlockingQueue<Job> jobs = new LinkedBlockingQueue<Job>();
		MRJobExecutionManager jobExecutor = Mockito.mock(MRJobExecutionManager.class);
		DHTConnectionProvider dhtConnectionProvider = Mockito.mock(DHTConnectionProvider.class);
		Mockito.when(jobExecutor.dhtConnectionProvider()).thenReturn(dhtConnectionProvider);
		Mockito.when(dhtConnectionProvider.peerAddress()).thenReturn(new PeerAddress(new Number160(1)));
		m = MRJobExecutorMessageConsumer.newInstance(jobs).jobExecutor(jobExecutor);

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
		Job job = Job.newInstance("TEST").nextProcedure(new WordCountMapper()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers)
				.inputPath(inputPath).maxFileSize(megaByte);

		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newMaxFileSizeTaskSplitter();
		splitter.split(job);
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());

		for (Task task : tasks) {
			task.updateStati(new PeerAddress(new Number160(1)), BCStatusType.EXECUTING_TASK);
			task.updateStati(new PeerAddress(new Number160(1)), BCStatusType.FINISHED_TASK);
			task.updateStati(new PeerAddress(new Number160(2)), BCStatusType.EXECUTING_TASK);
			task.updateStati(new PeerAddress(new Number160(2)), BCStatusType.FINISHED_TASK);
		}
		m.handleReceivedJob(job);

		String jobId = job.id();
		//
		// Job jobCopy = Job.newJob("TEST").nextProcedure(new WordCountMapper()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers)
		// .inputPath(inputPath).maxFileSize(megaByte);

		// splitter.split(jobCopy);
		BlockingQueue<Task> tasks2 = new LinkedBlockingQueue<Task>();
		for (Task task : tasks) {

			tasks2.add(task.copyWithoutExecutingPeers());

		}
		for (Task task : tasks2) {
			task.updateStati(new PeerAddress(new Number160(1)), BCStatusType.EXECUTING_TASK);
			task.updateStati(new PeerAddress(new Number160(1)), BCStatusType.FINISHED_TASK);
			task.updateStati(new PeerAddress(new Number160(1)), BCStatusType.EXECUTING_TASK);
			task.updateStati(new PeerAddress(new Number160(1)), BCStatusType.FINISHED_TASK);
			task.updateStati(new PeerAddress(new Number160(2)), BCStatusType.EXECUTING_TASK);

			task.updateStati(new PeerAddress(new Number160(2)), BCStatusType.FINISHED_TASK);
			m.handleTaskExecutionStatusUpdate(jobId, task.id(), new PeerAddress(new Number160(2)), BCStatusType.FINISHED_TASK);

			task.updateStati(new PeerAddress(new Number160(2)), BCStatusType.EXECUTING_TASK);
			m.handleTaskExecutionStatusUpdate(jobId, task.id(), new PeerAddress(new Number160(2)), BCStatusType.EXECUTING_TASK);

			// task.updateExecutingPeerStatus(new PeerAddress(new Number160(2)), JobStatus.FINISHED_TASK);

			task.updateStati(new PeerAddress(new Number160(3)), BCStatusType.EXECUTING_TASK);
			m.handleTaskExecutionStatusUpdate(jobId, task.id(), new PeerAddress(new Number160(3)), BCStatusType.EXECUTING_TASK);

			task.updateStati(new PeerAddress(new Number160(3)), BCStatusType.FINISHED_TASK);
			m.handleTaskExecutionStatusUpdate(jobId, task.id(), new PeerAddress(new Number160(3)), BCStatusType.FINISHED_TASK);
		}

		assertTrue(m.jobs().peek().tasks(0).peek().allAssignedPeers().size() == 3);
		System.err.println(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(1))).size());
		assertTrue(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(1))).size() == 1);
		assertTrue(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(2))).size() == 2);
		assertTrue(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(3))).size() == 1);
		assertTrue(new ArrayList<BCStatusType>(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(1)))).get(0)
				.equals(BCStatusType.FINISHED_TASK));
		assertTrue(new ArrayList<BCStatusType>(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(2)))).get(0)
				.equals(BCStatusType.FINISHED_TASK));
		assertTrue(new ArrayList<BCStatusType>(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(2)))).get(1)
				.equals(BCStatusType.EXECUTING_TASK));
		assertTrue(new ArrayList<BCStatusType>(m.jobs().peek().tasks(0).peek().statiForPeer(new PeerAddress(new Number160(3)))).get(0)
				.equals(BCStatusType.FINISHED_TASK));
		m.handleFinishedAllTasks(jobId, tasks2, new PeerAddress(new Number160(2)));

		BlockingQueue<Task> tasks3 = new LinkedBlockingQueue<Task>();
		for (Task task : tasks) {

			tasks3.add(task.copyWithoutExecutingPeers());
		}
		for (Task task : tasks3) {
			task.updateStati(new PeerAddress(new Number160(2)), BCStatusType.EXECUTING_TASK);
			task.updateStati(new PeerAddress(new Number160(2)), BCStatusType.FINISHED_TASK);
			task.updateStati(new PeerAddress(new Number160(2)), BCStatusType.EXECUTING_TASK);
			task.updateStati(new PeerAddress(new Number160(2)), BCStatusType.FINISHED_TASK);
		}

		m.handleFinishedAllTasks(jobId, tasks3, new PeerAddress(new Number160(3)));

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
				for (BCStatusType s : task.statiForPeer(new PeerAddress(new Number160(i)))) {
					assertTrue(s.equals(BCStatusType.FINISHED_TASK));
					System.err.println(s);
				}
			}
		}
	}

}
