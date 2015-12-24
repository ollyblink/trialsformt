package mapreduce.manager.broadcasthandler.messageconsumer;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.Tasks;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import mapreduce.utils.MaxFileSizeFileSplitter;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutorMessageConsumerTest {

	private static final String[] TEST_KEYS = { "hello", "world", "this", "is", "a", "test" };
	private static MRJobExecutionManagerMessageConsumer testMessageConsumer;
	private static PeerAddress peer1;
	private static PeerAddress peer2;
	private static PeerAddress peer3;
	private static Job job;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		peer1 = new PeerAddress(new Number160(1));
		peer2 = new PeerAddress(new Number160(2));
		peer3 = new PeerAddress(new Number160(3));
		List<Job> jobs = SyncedCollectionProvider.syncedList();
		MRJobExecutionManager jobExecutor = Mockito.mock(MRJobExecutionManager.class);
		DHTConnectionProvider dhtConnectionProvider = Mockito.mock(DHTConnectionProvider.class);
		Mockito.when(jobExecutor.dhtConnectionProvider()).thenReturn(dhtConnectionProvider);
		Mockito.when(dhtConnectionProvider.peerAddress()).thenReturn(peer1);
		testMessageConsumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(jobExecutor);
		resetJob();

		// m.updateTask(String jobId, String taskId, PeerAddress peerAddress, JobStatus currentStatus);
		// public void handleFinishedJob(String jobId, String jobSubmitterId);

	}

	private static void resetJob() {
		job = Job.create("TEST").nextProcedure(WordCountMapper.newInstance()).maxNrOfFinishedWorkersPerTask(5);
		ProcedureInformation currentProc = job.currentProcedure();

		for (String taskKey : TEST_KEYS) {
			currentProc.addTask(Task.newInstance(taskKey, job.id()));
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testHandleReceivedJob() {
		testMessageConsumer.jobs().clear();
		Job job = Job.create("TEST");
		testMessageConsumer.handleReceivedJob(job);
		assertEquals(1, testMessageConsumer.jobs().size());
		assertEquals(job.id(), testMessageConsumer.jobs().get(0).id());
		testMessageConsumer.handleReceivedJob(job);
		assertEquals(1, testMessageConsumer.jobs().size());
		assertEquals(job.id(), testMessageConsumer.jobs().get(0).id());
	}

	@Test
	public void testHandleTaskExecutionStatusUpdate() {
		resetJob();

		testMessageConsumer.jobs().clear();

		testMessageConsumer.handleReceivedJob(job);

		List<Task> copy = new ArrayList<Task>(TEST_KEYS.length);
		for (String taskKey : TEST_KEYS) {
			copy.add(Task.newInstance(taskKey, job.id()));
		}

		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0),
				TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0),
				TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1)));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0),
				TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0),
				TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1)));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0),
				TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK));
		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0),
				TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK));

		Job job1 = testMessageConsumer.jobs().get(0);
		List<Task> job1Tasks = job1.currentProcedure().tasks();
		assertEquals(2, Tasks.allAssignedPeers(job1Tasks.get(0)).size());
		assertEquals(peer1, Tasks.allAssignedPeers(job1Tasks.get(0)).get(0));
		assertEquals(2, Tasks.statiForPeer(job1Tasks.get(0), peer1).size());
		assertEquals(BCMessageStatus.FINISHED_TASK, Tasks.statiForPeer(job1Tasks.get(0), peer1).get(0));
		assertEquals(BCMessageStatus.EXECUTING_TASK, Tasks.statiForPeer(job1Tasks.get(0), peer1).get(1));

		testMessageConsumer.handleTaskExecutionStatusUpdate(copy.get(0),
				TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1)));

		assertEquals(BCMessageStatus.FINISHED_TASK, Tasks.statiForPeer(job1Tasks.get(0), peer1).get(1));

		assertEquals(peer2, Tasks.allAssignedPeers(job1Tasks.get(0)).get(1));
		assertEquals(1, Tasks.statiForPeer(job1Tasks.get(0), peer2).size());
		assertEquals(BCMessageStatus.FINISHED_TASK, Tasks.statiForPeer(job1Tasks.get(0), peer2).get(0));
	}

	@Test
	public void testTaskUpdatesAndFinishedAllTasks() {
		resetJob();

		testMessageConsumer.jobs().clear();
		List<Task> tasks = job.currentProcedure().tasks();
		for (Task task : tasks) {
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK),
					job.maxNrOfFinishedWorkersPerTask());
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1)),
					job.maxNrOfFinishedWorkersPerTask());
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK),
					job.maxNrOfFinishedWorkersPerTask());
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1)),
					job.maxNrOfFinishedWorkersPerTask());
		}
		testMessageConsumer.handleReceivedJob(job);
		assertEquals(1, testMessageConsumer.jobs().size());
		assertEquals(job.id(), testMessageConsumer.jobs().get(0).id());
		List<Task> tasks2 = testMessageConsumer.jobs().get(0).currentProcedure().tasks();
		for (Task task : tasks2) {
			assertEquals(peer1, Tasks.allAssignedPeers(task).get(0));
			assertEquals(BCMessageStatus.FINISHED_TASK, Tasks.statiForPeer(task, Tasks.allAssignedPeers(task).get(0)).get(0));
			assertEquals(peer2, Tasks.allAssignedPeers(task).get(1));
			assertEquals(BCMessageStatus.FINISHED_TASK, Tasks.statiForPeer(task, Tasks.allAssignedPeers(task).get(1)).get(0));
		}
		//
		List<Task> copy = new ArrayList<Task>(TEST_KEYS.length);
		for (String taskKey : TEST_KEYS) {
			copy.add(Task.newInstance(taskKey, job.id()));
		}
		int max = job.maxNrOfFinishedWorkersPerTask();
		for (Task task : copy) {
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK), max);
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1)), max);
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer1).status(BCMessageStatus.EXECUTING_TASK), max);
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer1).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1)), max);
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK), max);

			TaskResult next = TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1));
			Tasks.updateStati(task, next, max);
			testMessageConsumer.handleTaskExecutionStatusUpdate(task, next);

			next = TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK);
			Tasks.updateStati(task, next, max);
			testMessageConsumer.handleTaskExecutionStatusUpdate(task, next);

			next = TaskResult.newInstance().sender(peer3).status(BCMessageStatus.EXECUTING_TASK);
			Tasks.updateStati(task, next, max);
			testMessageConsumer.handleTaskExecutionStatusUpdate(task, next);

			next = TaskResult.newInstance().sender(peer3).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1));
			Tasks.updateStati(task, next, max);
			testMessageConsumer.handleTaskExecutionStatusUpdate(task, next);
		}

		Task taskToCheck = testMessageConsumer.jobs().get(0).currentProcedure().tasks().get(0);
		assertEquals(3, Tasks.allAssignedPeers(taskToCheck).size());
		assertEquals(1, Tasks.statiForPeer(taskToCheck, peer1).size());
		assertEquals(2, Tasks.statiForPeer(taskToCheck, peer2).size());
		assertEquals(1, Tasks.statiForPeer(taskToCheck, peer3).size());
		assertEquals(BCMessageStatus.FINISHED_TASK, Tasks.statiForPeer(taskToCheck, peer1).get(0));
		assertEquals(BCMessageStatus.FINISHED_TASK, Tasks.statiForPeer(taskToCheck, peer2).get(0));
		assertEquals(BCMessageStatus.EXECUTING_TASK, Tasks.statiForPeer(taskToCheck, peer2).get(1));
		assertEquals(BCMessageStatus.FINISHED_TASK, Tasks.statiForPeer(taskToCheck, peer3).get(0));

		testMessageConsumer.updateJob(job.copy(), peer2);

		List<Task> copy2 = new ArrayList<Task>(TEST_KEYS.length);
		for (String taskKey : TEST_KEYS) {
			copy2.add(Task.newInstance(taskKey, job.id()));
		}
		for (Task task : copy2) {
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK), max);
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1)), max);
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer2).status(BCMessageStatus.EXECUTING_TASK), max);
			Tasks.updateStati(task, TaskResult.newInstance().sender(peer2).status(BCMessageStatus.FINISHED_TASK).resultHash(new Number160(1)), max);
			System.err.println("Task is finished? " + task.isFinished());
		}

		testMessageConsumer.updateJob(job.copy(), peer3);
		List<Task> tasks3 = testMessageConsumer.jobs().get(0).currentProcedure().tasks();
		int cntr = 0;
		for (Task task : tasks3) {
			assertEquals(1, Tasks.allAssignedPeers(task).size());
			int number = 0;
			for (int i = 1; i <= 3; ++i) {
				if (i == 1 || i == 3) {
					number = 0;
				} else if (i == 2) {
					number = 2;
				}
				System.err.println(cntr++ + " i is " + i);
				assertEquals(number, Tasks.statiForPeer(task, new PeerAddress(new Number160(i))).size());
				for (BCMessageStatus s : Tasks.statiForPeer(task, new PeerAddress(new Number160(i)))) {
					assertEquals(BCMessageStatus.FINISHED_TASK, s);
					// System.err.println(s);
				}
			}
		}
	}

	@Ignore
	public void testHandleFinishedTaskComparion() {
		prepare(testMessageConsumer);
		ArrayList<Task> tasks2 = new ArrayList<Task>(
				testMessageConsumer.jobs().get(0).tasks(testMessageConsumer.jobs().get(0).currentProcedureIndex()));
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

		ITaskSplitter splitter = MaxFileSizeFileSplitter.create();
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
		}
	}

	@Ignore
	public void testHandleFinishedAllTaskComparion() {
		testMessageConsumer.jobs().clear();
		CopyOnWriteArrayList<Job> jobs = new CopyOnWriteArrayList<Job>();
		MRJobExecutionManager jobExecutor = Mockito.mock(MRJobExecutionManager.class);
		DHTConnectionProvider dhtConnectionProvider = Mockito.mock(DHTConnectionProvider.class);
		Mockito.when(jobExecutor.dhtConnectionProvider()).thenReturn(dhtConnectionProvider);
		Mockito.when(dhtConnectionProvider.peerAddress()).thenReturn(peer1);
		MRJobExecutionManagerMessageConsumer consumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(jobExecutor);
		prepare(consumer);

		ArrayList<Task> taskList = new ArrayList<Task>(consumer.jobs().get(0).tasks(consumer.jobs().get(0).currentProcedureIndex()));
		Job mockJob = Mockito.mock(Job.class);
		Mockito.when(mockJob.id()).thenReturn(jobs.get(0).id());
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

		assertEquals(null, new ArrayList<Task>(testMessageConsumer.jobs().get(0).tasks(testMessageConsumer.jobs().get(0).currentProcedureIndex()))
				.get(0).finalDataLocation());
		assertEquals(null, new ArrayList<Task>(testMessageConsumer.jobs().get(0).tasks(testMessageConsumer.jobs().get(0).currentProcedureIndex()))
				.get(1).finalDataLocation());
		assertEquals(null, new ArrayList<Task>(testMessageConsumer.jobs().get(0).tasks(testMessageConsumer.jobs().get(0).currentProcedureIndex()))
				.get(2).finalDataLocation());
		assertEquals(null, new ArrayList<Task>(testMessageConsumer.jobs().get(0).tasks(testMessageConsumer.jobs().get(0).currentProcedureIndex()))
				.get(3).finalDataLocation());
		testMessageConsumer.updateJob(consumer.jobs().get(0), peer2);

		ArrayList<Task> tasks2 = new ArrayList<Task>(
				testMessageConsumer.jobs().get(0).tasks(testMessageConsumer.jobs().get(0).currentProcedureIndex()));
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
