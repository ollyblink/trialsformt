package mapreduce.execution.jobtask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.junit.Test;

import mapreduce.execution.computation.standardprocedures.NullMapReduceProcedure;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class JobTest {

	@Test
	public void testNextProcedure() {
		List<Task> tasksForProcedure = new ArrayList<Task>();
		String jobId = IDCreator.INSTANCE.createTimeRandomID(Job.class.getSimpleName());
		tasksForProcedure.add(Task.newInstance(jobId));
		tasksForProcedure.add(Task.newInstance(jobId));
		tasksForProcedure.add(Task.newInstance(jobId));
		tasksForProcedure.add(Task.newInstance(jobId));
		Job job = Job.newInstance("ME").maxNrOfFinishedWorkersPerTask(3).nextProcedure(new WordCountMapper(), tasksForProcedure);
		assertTrue(job.currentProcedureIndex() == 0);
		job.incrementProcedureNumber();
		assertTrue(job.currentProcedureIndex() == 0);

		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		assertFalse(tasks == null);
		assertEquals(4, tasks.size());
		assertEquals("WordCountMapper", job.procedure(job.currentProcedureIndex()).toString());
		assertTrue(job.currentProcedureIndex() == 0);
		job.incrementProcedureNumber();
		assertTrue(job.currentProcedureIndex() == 0);

		List<Task> tasksForProcedure2 = new ArrayList<Task>();
		tasksForProcedure2.add(Task.newInstance(jobId));
		tasksForProcedure2.add(Task.newInstance(jobId));
		job.nextProcedure(NullMapReduceProcedure.newInstance(), tasksForProcedure2);

		assertTrue(job.currentProcedureIndex() == 0);
		job.incrementProcedureNumber();
		assertTrue(job.currentProcedureIndex() == 1);

		tasks = job.tasks(job.currentProcedureIndex());
		assertFalse(tasks == null);
		assertEquals("NullMapReduceProcedure", job.procedure(job.currentProcedureIndex()).toString());
		assertTrue(job.currentProcedureIndex() == 1);
		job.incrementProcedureNumber();
		assertTrue(job.currentProcedureIndex() == 1);

	}

	@Test
	public void testUpdateTaskStati() {
		String jobId = IDCreator.INSTANCE.createTimeRandomID(Job.class.getSimpleName());
		List<Task> tasksForProcedure = new ArrayList<Task>();
		tasksForProcedure.add(Task.newInstance(jobId));
		tasksForProcedure.add(Task.newInstance(jobId));
		Job job = Job.newInstance("ME").maxNrOfFinishedWorkersPerTask(3).nextProcedure(new WordCountMapper(), tasksForProcedure);
		ArrayList<Task> list = new ArrayList<Task>(tasksForProcedure);
		PeerAddress[] peers = new PeerAddress[3];
		peers[0] = new PeerAddress(Number160.createHash("1"));
		peers[1] = new PeerAddress(Number160.createHash("2"));

		job.updateTaskExecutionStatus(tasksForProcedure.get(0).id(),
				TaskResult.newInstance().sender(peers[0]).status(BCMessageStatus.EXECUTING_TASK));
		assertTrue(list.get(0).allAssignedPeers().contains(peers[0]));
		assertTrue(list.get(0).statiForPeer(peers[0]).contains(BCMessageStatus.EXECUTING_TASK));

		job.updateTaskExecutionStatus(tasksForProcedure.get(0).id(), TaskResult.newInstance().sender(peers[0]).status(BCMessageStatus.FINISHED_TASK));
		assertTrue(list.get(0).allAssignedPeers().contains(peers[0]));
		assertTrue(list.get(0).statiForPeer(peers[0]).contains(BCMessageStatus.FINISHED_TASK));

		job.updateTaskExecutionStatus(tasksForProcedure.get(1).id(),
				TaskResult.newInstance().sender(peers[1]).status(BCMessageStatus.EXECUTING_TASK));
		assertTrue(list.get(1).allAssignedPeers().contains(peers[1]));
		assertTrue(list.get(1).statiForPeer(peers[1]).contains(BCMessageStatus.EXECUTING_TASK));

		job.updateTaskExecutionStatus(tasksForProcedure.get(1).id(), TaskResult.newInstance().sender(peers[1]).status(BCMessageStatus.FINISHED_TASK));
		assertTrue(list.get(1).allAssignedPeers().contains(peers[1]));
		assertTrue(list.get(1).statiForPeer(peers[1]).contains(BCMessageStatus.FINISHED_TASK));

	}

}
