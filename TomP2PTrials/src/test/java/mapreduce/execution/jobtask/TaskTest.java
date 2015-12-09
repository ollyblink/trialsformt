package mapreduce.execution.jobtask;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.computation.standardprocedures.NullMapReduceProcedure;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.taskexecutorscleaner.IJobCleaner;
import mapreduce.execution.task.taskexecutorscleaner.TaskExecutorsCleaner;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.utils.IDCreator;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class TaskTest {

	private static Task task;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String jobId = IDCreator.INSTANCE.createTimeRandomID(Job.class.getSimpleName());

		task = Task.newInstance(jobId).maxNrOfFinishedWorkers(3);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testUpdateExecutingPeerStatus() {
		PeerAddress peerAddress = new PeerAddress(Number160.ZERO);
		TaskResult tR = TaskResult.newInstance().sender(peerAddress).resultHash(new Number160(1));
		// Wrong job status 
		task.updateStati(tR.status(BCMessageStatus.DISTRIBUTED_JOB));
		assertEquals(0, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.FINISHED_ALL_TASKS));
		assertEquals(0, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.TASK_FAILED));
		assertEquals(0, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());
		// Finish wrong job status

		task.updateStati(tR.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		// Wrong job status
		task.updateStati(tR.status(BCMessageStatus.DISTRIBUTED_JOB));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.FINISHED_ALL_TASKS));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.TASK_FAILED));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());
		// Finish wrong job status

		task.updateStati(tR.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(true, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(true, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(true, task.isFinished());
		// Second peer
		 
		task.maxNrOfFinishedWorkers(5);
 
		PeerAddress peerAddress2 = new PeerAddress(Number160.ONE);
		TaskResult tR2 = TaskResult.newInstance().sender(peerAddress2).resultHash(new Number160(1)).status(BCMessageStatus.DISTRIBUTED_JOB);
		// Wrong job status
		task.updateStati(tR2);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.FINISHED_ALL_TASKS));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.TASK_FAILED));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());
		// Finish wrong job status

		task.updateStati(tR2.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		// Wrong job status
		task.updateStati(tR2.status(BCMessageStatus.DISTRIBUTED_JOB));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.FINISHED_ALL_TASKS));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.TASK_FAILED));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());
		// Finish wrong job status

		task.updateStati(tR2.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(true, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(true, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(true, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(true, task.isFinished());

		 
		// third peer
		 
		task.maxNrOfFinishedWorkers(7);
 
		PeerAddress peerAddress3 = new PeerAddress(Number160.createHash(3)); 
		TaskResult tR3 = TaskResult.newInstance().sender(peerAddress3).resultHash(new Number160(1));;
		task.updateStati(tR3.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress3, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress3, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(2, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(2, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR.status(BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(3, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(3, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(false, task.isFinished());

		task.updateStati(tR2.status(BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(2, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(4, task.totalNumberOfFinishedExecutions());
		assertEquals(2, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());
		assertEquals(true, task.isFinished());
 
		TaskExecutorsCleaner cleaner = TaskExecutorsCleaner.newInstance();
		cleaner.cleanUp(Job.newInstance("").nextProcedure(NullMapReduceProcedure.newInstance(), task));
	}

}
