package mapreduce.execution.jobtask;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class TaskTest {

	private static Task task;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String jobId = IDCreator.INSTANCE.createTimeRandomID(Job.class.getSimpleName());

		task = Task.newInstance(jobId);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testUpdateExecutingPeerStatus() {
		PeerAddress peerAddress = new PeerAddress(Number160.ZERO);
		// Wrong job status
		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.DISTRIBUTED_JOB));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.FINISHED_ALL_TASKS));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.TASK_FAILED));
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
		// Finish wrong job status

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.FINISHED_TASK));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.EXECUTING_TASK));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.EXECUTING_TASK));
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

		// Wrong job status
		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.DISTRIBUTED_JOB));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.FINISHED_ALL_TASKS));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.TASK_FAILED));
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
		// Finish wrong job status

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.FINISHED_TASK));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.FINISHED_TASK));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.EXECUTING_TASK));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.FINISHED_TASK));
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

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		// Second peer

		PeerAddress peerAddress2 = new PeerAddress(Number160.ONE);
		// Wrong job status
		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.DISTRIBUTED_JOB));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.FINISHED_ALL_TASKS));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.TASK_FAILED));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		// Finish wrong job status

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		// Wrong job status
		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.DISTRIBUTED_JOB));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.FINISHED_ALL_TASKS));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.TASK_FAILED));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		// Finish wrong job status

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(4, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(4, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(4, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(5, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(5, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(6, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		// third peer

		PeerAddress peerAddress3 = new PeerAddress(Number160.createHash(3));
		task.updateStati(Tuple.create(peerAddress3, BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress3, BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress3, BCMessageStatus.FINISHED_TASK));
		assertEquals(6, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(2, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfSameStatiForPeer(peerAddress2, BCMessageStatus.FINISHED_TASK));
		assertEquals(6, task.totalNumberOfFinishedExecutions());
		assertEquals(2, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateStati(Tuple.create(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(3, task.numberOfPeersWithSingleStatus(BCMessageStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(BCMessageStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(BCMessageStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfSameStatiForPeer(peerAddress, BCMessageStatus.FINISHED_TASK));
		assertEquals(6, task.totalNumberOfFinishedExecutions());
		assertEquals(3, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

	}

}
