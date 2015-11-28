package mapreduce.execution.jobtask;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.utils.IDCreator;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class TaskTest {

	private static Task task;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String jobId = IDCreator.INSTANCE.createTimeRandomID(Job.class.getSimpleName());

		task = Task.newTask(jobId);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testUpdateExecutingPeerStatus() {
		PeerAddress peerAddress = new PeerAddress(Number160.ZERO);
		// Wrong job status
		task.updateExecutingPeerStatus(peerAddress, JobStatus.DISTRIBUTED_JOB);
		assertEquals(0, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.FINISHED_ALL_TASKS);
		assertEquals(0, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.TASK_FAILED);
		assertEquals(0, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		// Finish wrong job status

		task.updateExecutingPeerStatus(peerAddress, JobStatus.FINISHED_TASK);
		assertEquals(0, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.EXECUTING_TASK);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.EXECUTING_TASK);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());

		// Wrong job status
		task.updateExecutingPeerStatus(peerAddress, JobStatus.DISTRIBUTED_JOB);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.FINISHED_ALL_TASKS);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.TASK_FAILED);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(0, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(0, task.numberOfPeersWithAtLeastOneFinishedExecution());
		// Finish wrong job status

		task.updateExecutingPeerStatus(peerAddress, JobStatus.FINISHED_TASK);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(1, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.FINISHED_TASK);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(1, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.EXECUTING_TASK);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(1, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.FINISHED_TASK);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.EXECUTING_TASK);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(2, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.FINISHED_TASK);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		// Second peer

		PeerAddress peerAddress2 = new PeerAddress(Number160.ONE);
		// Wrong job status
		task.updateExecutingPeerStatus(peerAddress2, JobStatus.DISTRIBUTED_JOB);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.FINISHED_ALL_TASKS);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.TASK_FAILED);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		// Finish wrong job status

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.FINISHED_TASK);
		assertEquals(1, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.EXECUTING_TASK);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.EXECUTING_TASK);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		// Wrong job status
		task.updateExecutingPeerStatus(peerAddress2, JobStatus.DISTRIBUTED_JOB);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.FINISHED_ALL_TASKS);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.TASK_FAILED);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(3, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(1, task.numberOfPeersWithAtLeastOneFinishedExecution());
		// Finish wrong job status

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.FINISHED_TASK);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(4, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.FINISHED_TASK);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(4, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.EXECUTING_TASK);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(4, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.FINISHED_TASK);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(5, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.EXECUTING_TASK);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(5, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.FINISHED_TASK);
		assertEquals(2, task.numberOfDifferentPeersExecutingTask());
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(6, task.totalNumberOfFinishedExecutions());
		assertEquals(0, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		// third peer

		PeerAddress peerAddress3 = new PeerAddress(Number160.createHash(3));
		task.updateExecutingPeerStatus(peerAddress3, JobStatus.EXECUTING_TASK);
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(1, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress3, JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfSameStatiForPeer(peerAddress3, JobStatus.FINISHED_TASK));
		assertEquals(6, task.totalNumberOfFinishedExecutions());
		assertEquals(1, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress2, JobStatus.EXECUTING_TASK);
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(2, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfSameStatiForPeer(peerAddress2, JobStatus.FINISHED_TASK));
		assertEquals(6, task.totalNumberOfFinishedExecutions());
		assertEquals(2, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

		task.updateExecutingPeerStatus(peerAddress, JobStatus.EXECUTING_TASK);
		assertEquals(3, task.numberOfDifferentPeersExecutingTask());
		assertEquals(3, task.numberOfPeersWithSingleStatus(JobStatus.EXECUTING_TASK));
		assertEquals(0, task.numberOfPeersWithSingleStatus(JobStatus.FINISHED_TASK));
		assertEquals(0, task.numberOfPeersWithMultipleSameStati(JobStatus.EXECUTING_TASK));
		assertEquals(2, task.numberOfPeersWithMultipleSameStati(JobStatus.FINISHED_TASK));
		assertEquals(1, task.numberOfSameStatiForPeer(peerAddress, JobStatus.EXECUTING_TASK));
		assertEquals(3, task.numberOfSameStatiForPeer(peerAddress, JobStatus.FINISHED_TASK));
		assertEquals(6, task.totalNumberOfFinishedExecutions());
		assertEquals(3, task.totalNumberOfCurrentExecutions());
		assertEquals(2, task.numberOfPeersWithAtLeastOneFinishedExecution());

	}

}
