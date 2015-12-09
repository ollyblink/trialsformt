package mapreduce.manager.broadcasthandler.messageconsumer;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.mockito.Mockito;

import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.tasksplitting.ITaskSplitter;
import mapreduce.execution.task.tasksplitting.MaxFileSizeTaskSplitter;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.FileSizes;
import mapreduce.utils.FileUtils;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutorMessageConsumerTest {

	private static MRJobExecutionManagerMessageConsumer testMessageConsumer;
	private static PeerAddress peer1;
	private static PeerAddress peer2;
	private static PeerAddress peer3;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		peer1 = new PeerAddress(new Number160(1));
		peer2 = new PeerAddress(new Number160(2));
		peer3 = new PeerAddress(new Number160(3));
		BlockingQueue<Job> jobs = new LinkedBlockingQueue<Job>();
		MRJobExecutionManager jobExecutor = Mockito.mock(MRJobExecutionManager.class);
		DHTConnectionProvider dhtConnectionProvider = Mockito.mock(DHTConnectionProvider.class);
		Mockito.when(jobExecutor.dhtConnectionProvider()).thenReturn(dhtConnectionProvider);
		Mockito.when(dhtConnectionProvider.peerAddress()).thenReturn(peer1);
		testMessageConsumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(jobExecutor);

		// m.updateTask(String jobId, String taskId, PeerAddress peerAddress, JobStatus currentStatus);
		// public void handleFinishedJob(String jobId, String jobSubmitterId);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testHandleReceivedJob() {
		testMessageConsumer.jobs().clear();
		Job job = Job.newInstance("TEST");
		testMessageConsumer.handleReceivedJob(job);
		assertEquals(1, testMessageConsumer.jobs().size());
		assertEquals(job.id(), testMessageConsumer.jobs().peek().id());
		testMessageConsumer.handleReceivedJob(job);
		assertEquals(1, testMessageConsumer.jobs().size());
		assertEquals(job.id(), testMessageConsumer.jobs().peek().id());
	}

	@Test
	public void testHandleTaskExecutionStatusUpdate() {
		testMessageConsumer.jobs().clear();
		String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/task/tasksplitting/testfile";
		if (new File(inputPath + "/tmp").exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));
		}

		Job job = Job.newInstance("TEST").nextProcedure(new WordCountMapper()).inputPath(inputPath).maxFileSize(FileSizes.KILO_BYTE.value()).maxNrOfFinishedWorkersPerTask(5);

		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newInstance();
		splitter.split(job);
		testMessageConsumer.handleReceivedJob(job);
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		List<Task> copy = new ArrayList<Task>(tasks.size());
		for (Task task : tasks) {
			Task tCopy = Mockito.mock(Task.class);
			Mockito.when(tCopy.id()).thenReturn(task.id());
			Mockito.when(tCopy.jobId()).thenReturn(task.jobId());
			copy.add(tCopy);
		}

		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0), TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0), TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0), TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0), TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0), TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0), TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK));

		Job job1 = testMessageConsumer.jobs().peek();
		assertEquals(2, job1.tasks(job1.currentProcedureIndex()).peek().allAssignedPeers().size());
		assertEquals(peer1, job1.tasks(job1.currentProcedureIndex()).peek().allAssignedPeers().get(0));
		assertEquals(2, job1.tasks(job1.currentProcedureIndex()).peek().statiForPeer(peer1).size());
		assertEquals(BCMessageStatus.FINISHED_TASK, job1.tasks(job1.currentProcedureIndex()).peek().statiForPeer(peer1).get(0));
		assertEquals(BCMessageStatus.EXECUTING_TASK, job1.tasks(job1.currentProcedureIndex()).peek().statiForPeer(peer1).get(1));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0), TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK));
		assertEquals(BCMessageStatus.FINISHED_TASK, job1.tasks(job1.currentProcedureIndex()).peek().statiForPeer(peer1).get(1));

		assertEquals(peer2, job1.tasks(job1.currentProcedureIndex()).peek().allAssignedPeers().get(1));
		assertEquals(1, job1.tasks(job1.currentProcedureIndex()).peek().statiForPeer(peer2).size());
		assertEquals(BCMessageStatus.FINISHED_TASK, job1.tasks(job1.currentProcedureIndex()).peek().statiForPeer(peer2).get(0));
	}

	@Test
	public void testTaskUpdatesAndFinishedAllTasks()
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

		testMessageConsumer.jobs().clear();
		String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/task/tasksplitting/testfile";
		if (new File(inputPath + "/tmp").exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));
		}

		long megaByte = FileSizes.MEGA_BYTE.value();

		int maxNumberOfFinishedPeers = 5;
		Job job = Job.newInstance("TEST").nextProcedure(new WordCountMapper()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers)
				.inputPath(inputPath).maxFileSize(megaByte);

		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newInstance();
		splitter.split(job);
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());

		for (Task task : tasks) {
			task.updateStati(TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK));
		}
		testMessageConsumer.handleReceivedJob(job);
		assertEquals(1, testMessageConsumer.jobs().size());
		assertEquals(job.id(), testMessageConsumer.jobs().peek().id());
		BlockingQueue<Task> setTasks = testMessageConsumer.jobs().peek().tasks(testMessageConsumer.jobs().peek().currentProcedureIndex());
		for (Task task : setTasks) {
			assertEquals(peer1, task.allAssignedPeers().get(0));
			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(task.allAssignedPeers().get(0)).get(0));
			assertEquals(peer2, task.allAssignedPeers().get(1));
			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(task.allAssignedPeers().get(1)).get(0));
		}
		//
		Job jobCopy = Job.newInstance("TEST").nextProcedure(new WordCountMapper()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers)
				.inputPath(inputPath).maxFileSize(megaByte);
		Field idField = jobCopy.getClass().getDeclaredField("id");
		idField.setAccessible(true);
		idField.set(jobCopy, job.id());

		splitter.split(jobCopy);

		for (Task task : tasks) {
			jobCopy.tasks(jobCopy.currentProcedureIndex()).add(task.copyWithoutExecutingPeers());
		}
		for (Task task : jobCopy.tasks(jobCopy.currentProcedureIndex())) {
			task.updateStati(TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK));

			TaskResult next = TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK);
			task.updateStati(next);
			testMessageConsumer.handleTaskExecutionStatusUpdate(task, next);

			next = TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK);
			task.updateStati(next);
			testMessageConsumer.handleTaskExecutionStatusUpdate(task, next);

			next = TaskResult.newInstance().sender(peer3).status(BCMessageStatus.EXECUTING_TASK);
			task.updateStati(next);
			testMessageConsumer.handleTaskExecutionStatusUpdate(task, next);

			next = TaskResult.newInstance().sender(peer3).status(BCMessageStatus.FINISHED_TASK);
			task.updateStati(next);
			testMessageConsumer.handleTaskExecutionStatusUpdate(task, next);
		}

		assertEquals(3, testMessageConsumer.jobs().peek().tasks(0).peek().allAssignedPeers().size());
		assertEquals(1, testMessageConsumer.jobs().peek().tasks(0).peek().statiForPeer(peer1).size());
		assertEquals(2, testMessageConsumer.jobs().peek().tasks(0).peek().statiForPeer(peer2).size());
		assertEquals(1, testMessageConsumer.jobs().peek().tasks(0).peek().statiForPeer(peer3).size());
		assertEquals(BCMessageStatus.FINISHED_TASK, testMessageConsumer.jobs().peek().tasks(0).peek().statiForPeer(peer1).get(0));
		assertEquals(BCMessageStatus.FINISHED_TASK, testMessageConsumer.jobs().peek().tasks(0).peek().statiForPeer(peer2).get(0));
		assertEquals(BCMessageStatus.EXECUTING_TASK, testMessageConsumer.jobs().peek().tasks(0).peek().statiForPeer(peer2).get(1));
		assertEquals(BCMessageStatus.FINISHED_TASK, testMessageConsumer.jobs().peek().tasks(0).peek().statiForPeer(peer3).get(0));

		testMessageConsumer.updateJob(jobCopy, BCMessageStatus.FINISHED_ALL_TASKS, peer2);

		Job jobCopy2 = Job.newInstance("TEST").nextProcedure(new WordCountMapper()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers)
				.inputPath(inputPath).maxFileSize(megaByte);
		// Field idField = jobCopy2.getClass().getDeclaredField("id");
		// idField.setAccessible(true);
		idField.set(jobCopy2, job.id());

		splitter.split(jobCopy2);

		for (Task task : tasks) {
			jobCopy2.tasks(jobCopy2.currentProcedureIndex()).add(task.copyWithoutExecutingPeers());
		}
		for (Task task : jobCopy2.tasks(jobCopy2.currentProcedureIndex())) {
			task.updateStati(TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK));
		}

		testMessageConsumer.updateJob(jobCopy2, BCMessageStatus.FINISHED_ALL_TASKS, peer3);

		for (Task task : testMessageConsumer.jobs().peek().tasks(0)) {
			assertEquals(1, task.allAssignedPeers().size());
			System.err.println(task.allAssignedPeers().size());
			int number = 0;
			for (int i = 1; i <= 3; ++i) {
				if (i == 1 || i == 3) {
					number = 0;
				} else if (i == 2)
					number = 2;
				assertEquals(number, task.statiForPeer(new PeerAddress(new Number160(i))).size());
				for (BCMessageStatus s : task.statiForPeer(new PeerAddress(new Number160(i)))) {
					assertEquals(BCMessageStatus.FINISHED_TASK, s);
//					System.err.println(s);
				}
			}
		}
	}

	@Test
	public void testHandleFinishedTaskComparion() {
		prepare(testMessageConsumer);
		ArrayList<Task> tasks2 = new ArrayList<Task>(
				testMessageConsumer.jobs().peek().tasks(testMessageConsumer.jobs().peek().currentProcedureIndex()));
		assertEquals(peer1, tasks2.get(0).finalDataLocation().first());
		assertEquals(new Integer(2), tasks2.get(0).finalDataLocation().second());

		assertEquals(peer3, tasks2.get(1).finalDataLocation().first());
		assertEquals(new Integer(3), tasks2.get(1).finalDataLocation().second());

		assertEquals(peer2, tasks2.get(2).finalDataLocation().first());
		assertEquals(new Integer(1), tasks2.get(2).finalDataLocation().second());

		assertEquals(peer3, tasks2.get(3).finalDataLocation().first());
		assertEquals(new Integer(2), tasks2.get(3).finalDataLocation().second());
	}

	private void prepare(MRJobExecutionManagerMessageConsumer consumer) {
		consumer.jobs().clear();

		Job job = TestUtils.testJob();

		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newInstance();
		splitter.split(job);
		consumer.handleReceivedJob(job);
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		int cnt = 0;
		for (Task task : tasks) {
			Task tCopy = Mockito.mock(Task.class);
			Mockito.when(tCopy.id()).thenReturn(task.id());
			Mockito.when(tCopy.id()).thenReturn(task.id());
			Mockito.when(tCopy.jobId()).thenReturn(task.jobId());
			Tuple<PeerAddress, Integer> tuple = null;
			switch (cnt) {
			case 0:
				tuple = Tuple.create(peer1, 2);
				break;
			case 1:
				tuple = Tuple.create(peer3, 3);
				break;
			case 2:
				tuple = Tuple.create(peer2, 1);
				break;
			case 3:
				tuple = Tuple.create(peer3, 2);
				break;
			default:
				break;
			}
			++cnt;
			Mockito.when(tCopy.finalDataLocation()).thenReturn(tuple);
			consumer.handleFinishedTaskComparion(tCopy);
		}
	}

	@Test
	public void testHandleFinishedAllTaskComparion() {
		testMessageConsumer.jobs().clear();
		BlockingQueue<Job> jobs = new LinkedBlockingQueue<Job>();
		MRJobExecutionManager jobExecutor = Mockito.mock(MRJobExecutionManager.class);
		DHTConnectionProvider dhtConnectionProvider = Mockito.mock(DHTConnectionProvider.class);
		Mockito.when(jobExecutor.dhtConnectionProvider()).thenReturn(dhtConnectionProvider);
		Mockito.when(dhtConnectionProvider.peerAddress()).thenReturn(peer1);
		MRJobExecutionManagerMessageConsumer consumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(jobExecutor);
		prepare(consumer);

		ArrayList<Task> taskList = new ArrayList<Task>(consumer.jobs().peek().tasks(consumer.jobs().peek().currentProcedureIndex()));
		Job mockJob = Mockito.mock(Job.class);
		Mockito.when(mockJob.id()).thenReturn(jobs.peek().id());
		BlockingQueue<Task> mockTasks = new LinkedBlockingQueue<Task>();
		for (Task task : taskList) {
			Task tCopy = Mockito.mock(Task.class);
			Mockito.when(tCopy.id()).thenReturn(task.id());
			Mockito.when(tCopy.jobId()).thenReturn(task.jobId());
			Mockito.when(tCopy.finalDataLocation()).thenReturn(null);
			mockTasks.add(tCopy);
		}
		Mockito.when(mockJob.tasks(0)).thenReturn(mockTasks);
		testMessageConsumer.handleReceivedJob(mockJob);

		assertEquals(null, new ArrayList<Task>(testMessageConsumer.jobs().peek().tasks(testMessageConsumer.jobs().peek().currentProcedureIndex()))
				.get(0).finalDataLocation());
		assertEquals(null, new ArrayList<Task>(testMessageConsumer.jobs().peek().tasks(testMessageConsumer.jobs().peek().currentProcedureIndex()))
				.get(1).finalDataLocation());
		assertEquals(null, new ArrayList<Task>(testMessageConsumer.jobs().peek().tasks(testMessageConsumer.jobs().peek().currentProcedureIndex()))
				.get(2).finalDataLocation());
		assertEquals(null, new ArrayList<Task>(testMessageConsumer.jobs().peek().tasks(testMessageConsumer.jobs().peek().currentProcedureIndex()))
				.get(3).finalDataLocation());
		testMessageConsumer.updateJob(consumer.jobs().peek(), BCMessageStatus.FINISHED_ALL_TASK_COMPARIONS, peer2);

		ArrayList<Task> tasks2 = new ArrayList<Task>(
				testMessageConsumer.jobs().peek().tasks(testMessageConsumer.jobs().peek().currentProcedureIndex()));
		assertEquals(peer1, tasks2.get(0).finalDataLocation().first());
		assertEquals(new Integer(2), tasks2.get(0).finalDataLocation().second());

		assertEquals(peer3, tasks2.get(1).finalDataLocation().first());
		assertEquals(new Integer(3), tasks2.get(1).finalDataLocation().second());

		assertEquals(peer2, tasks2.get(2).finalDataLocation().first());
		assertEquals(new Integer(1), tasks2.get(2).finalDataLocation().second());

		assertEquals(peer3, tasks2.get(3).finalDataLocation().first());
		assertEquals(new Integer(2), tasks2.get(3).finalDataLocation().second());
	}
}
