package mapreduce.execution.task;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ListMultimap;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.utils.IDCreator;
import net.tomp2p.peers.Number160;

public class TaskTest {

	private static int NUMBER_OF_FINISHED_WORKERS = 3;
	private static Task task;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String jobId = IDCreator.INSTANCE.createTimeRandomID(Job.class.getSimpleName());

		task = Task.newInstance(jobId, "word");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testUpdateExecutingPeerStatus() {
		String peerAddress = "1";
		TaskResult tR = TaskResult.newInstance().sender(peerAddress).resultHash(new Number160(1));
		// Wrong job status
		Tasks.updateStati(task, tR.status(BCMessageStatus.DISTRIBUTED_JOB), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(0, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(0, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.FINISHED_PROCEDURE), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(0, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(0, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.FAILED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(0, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());
		// Finish wrong job status

		Tasks.updateStati(task, tR.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(0, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(0, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(0, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		// Wrong job status
		Tasks.updateStati(task, tR.status(BCMessageStatus.DISTRIBUTED_JOB), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(0, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.FINISHED_PROCEDURE), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(0, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.FAILED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(0, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());
		// Finish wrong job status

		Tasks.updateStati(task, tR.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(true, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(true, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(true, task.isFinished());

		ListMultimap<String, BCMessageStatus> executingPeers = task.executingPeers();
		for (String p : executingPeers.keySet()) {
			System.err.println(p + ": " + executingPeers.get(p));
		}
		// Second peer
		NUMBER_OF_FINISHED_WORKERS = 5;
		task.isFinished(false);

		String peerAddress2 = "2";
		TaskResult tR2 = TaskResult.newInstance().sender(peerAddress2).resultHash(new Number160(1)).status(BCMessageStatus.DISTRIBUTED_JOB);
		// Wrong job status
		Tasks.updateStati(task, tR2, NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.FINISHED_PROCEDURE), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.FAILED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());
		// Finish wrong job status

		Tasks.updateStati(task, tR2.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(1, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(2, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(2, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		// Wrong job status
		Tasks.updateStati(task, tR2.status(BCMessageStatus.DISTRIBUTED_JOB), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(2, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.FINISHED_PROCEDURE), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(2, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.FAILED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(2, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(1, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());
		// Finish wrong job status

		Tasks.updateStati(task, tR2.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(2, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(2, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(true, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(2, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(2, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(true, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(2, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(2, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(true, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(2, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(0, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(2, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(true, task.isFinished());
		for (String p : executingPeers.keySet()) {
			System.err.println(p + ": " + executingPeers.get(p));
		}
		// third peer

		NUMBER_OF_FINISHED_WORKERS = 7;
		task.isFinished(false);

		String peerAddress3 = "3";
		TaskResult tR3 = TaskResult.newInstance().sender(peerAddress3).resultHash(new Number160(1));

		Tasks.updateStati(task, tR3.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(3, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress3, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress3, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(1, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(2, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(3, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(2, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(2, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(2, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR.status(BCMessageStatus.EXECUTING_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(3, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(3, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, Tasks.numberOfSameStatiForPeer(task, peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(3, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(2, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(false, task.isFinished());

		Tasks.updateStati(task, tR2.status(BCMessageStatus.FINISHED_TASK), NUMBER_OF_FINISHED_WORKERS);
		assertEquals(3, Tasks.numberOfDifferentPeersExecutingOrFinishedTask(task));
		assertEquals(2, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, Tasks.numberOfPeersWithSingleStatus(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, Tasks.numberOfPeersWithMultipleSameStati(task, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, Tasks.numberOfSameStatiForPeer(task, peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(4, Tasks.totalNumberOfFinishedExecutions(task));
		assertEquals(2, Tasks.totalNumberOfCurrentExecutions(task));
		assertEquals(2, Tasks.numberOfPeersWithAtLeastOneFinishedExecution(task));
		assertEquals(true, task.isFinished());

		for (String p : executingPeers.keySet()) {
			System.err.println(p + ": " + executingPeers.get(p));
		}
	}

}
