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
import mapreduce.execution.job.PriorityLevel;
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
	private static String peer1;
	private static String peer2;
	private static String peer3;
	private static Job job;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		peer1 = "EXECUTOR_1";
		peer2 = "EXECUTOR_2";
		peer3 = "EXECUTOR_3";
		List<Job> jobs = SyncedCollectionProvider.syncedArrayList();
		MRJobExecutionManager jobExecutor = Mockito.mock(MRJobExecutionManager.class);
		DHTConnectionProvider dhtConnectionProvider = Mockito.mock(DHTConnectionProvider.class);
		Mockito.when(jobExecutor.dhtConnectionProvider()).thenReturn(dhtConnectionProvider);
		Mockito.when(dhtConnectionProvider.owner()).thenReturn(peer1);
		testMessageConsumer = MRJobExecutionManagerMessageConsumer.newInstance(jobs).jobExecutor(jobExecutor);
		resetJob();

		// m.updateTask(String jobId, String taskId, PeerAddress peerAddress, JobStatus currentStatus);
		// public void handleFinishedJob(String jobId, String jobSubmitterId);

	}

	private static void resetJob() {
		job = Job.create("TEST", PriorityLevel.MODERATE).addSubsequentProcedure(WordCountMapper.newInstance()).maxNrOfFinishedWorkersPerTask(5);
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
		Job job = Job.create("TEST", PriorityLevel.MODERATE);
 
		testMessageConsumer.handleReceivedJob(job);
		assertEquals(1, testMessageConsumer.jobs().size());
		assertEquals(job.id(), testMessageConsumer.jobs().get(0).id());
		testMessageConsumer.handleReceivedJob(job);
		assertEquals(1, testMessageConsumer.jobs().size());
		assertEquals(job.id(), testMessageConsumer.jobs().get(0).id());
		
		Job job2 = Job.create("TEST", PriorityLevel.HIGH);
		testMessageConsumer.handleReceivedJob(job2);
		assertEquals(2, testMessageConsumer.jobs().size());
		assertEquals(job2.id(), testMessageConsumer.jobs().get(0).id());
		assertEquals(job.id(), testMessageConsumer.jobs().get(1).id());
		
		
		Job job3 = Job.create("TEST", PriorityLevel.LOW);
		testMessageConsumer.handleReceivedJob(job3);
		assertEquals(3, testMessageConsumer.jobs().size());
		assertEquals(job2.id(), testMessageConsumer.jobs().get(0).id());
		assertEquals(job.id(), testMessageConsumer.jobs().get(1).id());
		assertEquals(job3.id(), testMessageConsumer.jobs().get(2).id());
		
		Job job4 = Job.create("TEST", PriorityLevel.MODERATE);
		testMessageConsumer.handleReceivedJob(job4);

		assertEquals(4, testMessageConsumer.jobs().size()); 
		assertEquals(job2.id(), testMessageConsumer.jobs().get(0).id()); 
		assertEquals(job3.id(), testMessageConsumer.jobs().get(3).id());
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

		Job jobCopy1 = job.copy();
		jobCopy1.currentProcedure().tasks(copy);
		testMessageConsumer.handleFinishedAllTasks(jobCopy1);

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

		Job jobCopy2 = job.copy();
		jobCopy2.currentProcedure().tasks(copy2);
		testMessageConsumer.handleFinishedAllTasks(jobCopy2);

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
				assertEquals(number, Tasks.statiForPeer(task, "EXECUTOR_"+i).size());
				for (BCMessageStatus s : Tasks.statiForPeer(task, "EXECUTOR_"+i)) {
					assertEquals(BCMessageStatus.FINISHED_TASK, s);
				}
			}
		}
	}

}
