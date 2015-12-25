package mapreduce.execution.jobtask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ListMultimap;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.computation.standardprocedures.NullMapReduceProcedure;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.Tasks;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.utils.IDCreator;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class JobTest {

	@Test
	public void testNextProcedure() {
		int counter = 1;
		List<Task> tasksForProcedure = new ArrayList<Task>();
		String jobId = IDCreator.INSTANCE.createTimeRandomID(Job.class.getSimpleName());
		tasksForProcedure.add(Task.newInstance(jobId, ("word" + counter++)));
		tasksForProcedure.add(Task.newInstance(jobId, ("word" + counter++)));
		tasksForProcedure.add(Task.newInstance(jobId, ("word" + counter++)));
		tasksForProcedure.add(Task.newInstance(jobId, ("word" + counter++)));
		Job job = Job.create("ME").maxNrOfFinishedWorkersPerTask(3).addSubsequentProcedure(WordCountMapper.newInstance());
		job.procedure(job.currentProcedureIndex()).tasks(tasksForProcedure);
		assertTrue(job.currentProcedureIndex() == 0);

		List<Task> tasks = job.procedure(job.currentProcedureIndex()).tasks();
		assertFalse(tasks == null);
		assertEquals(4, tasks.size());
		assertEquals("WordCountMapper", job.procedure(job.currentProcedureIndex()).procedure().getClass().getSimpleName());
		assertTrue(job.currentProcedureIndex() == 0);

		List<Task> tasksForProcedure2 = new ArrayList<Task>();
		tasksForProcedure2.add(Task.newInstance(jobId, ("word" + counter++)));
		tasksForProcedure2.add(Task.newInstance(jobId, ("word" + counter++)));
		job.addSubsequentProcedure(NullMapReduceProcedure.newInstance());
		job.procedure(job.currentProcedureIndex()).tasks(tasksForProcedure2);

		assertTrue(job.currentProcedureIndex() == 0);
		job.incrementCurrentProcedureIndex();
		assertTrue(job.currentProcedureIndex() == 1);

		tasks = job.procedure(job.currentProcedureIndex()).tasks();
		assertFalse(tasks == null);
		assertEquals("NullMapReduceProcedure", job.procedure(job.currentProcedureIndex()).procedure().getClass().getSimpleName());
		assertTrue(job.currentProcedureIndex() == 1);

	}

	@Test
	public void testUpdateTaskStati() {
		int counter = 0;

		Job job = Job.create("ME").maxNrOfFinishedWorkersPerTask(3).addSubsequentProcedure(NullMapReduceProcedure.newInstance());
		List<Task> list = new ArrayList<Task>();
		list.add(Task.newInstance("word" + (counter++), job.id()));
		list.add(Task.newInstance("word" + (counter++), job.id()));
		job.procedure(job.currentProcedureIndex()).tasks(list);

		PeerAddress[] peers = new PeerAddress[3];
		peers[0] = new PeerAddress(new Number160(1));
		peers[1] = new PeerAddress(new Number160(2));

		ProcedureInformation procInfo = job.procedure(job.currentProcedureIndex());
		procInfo.updateTaskExecutionStatus(list.get(0).id(), TaskResult.newInstance().sender(peers[0]).status(BCMessageStatus.EXECUTING_TASK),
				job.maxNrOfFinishedWorkersPerTask());
		assertTrue(Tasks.allAssignedPeers(list.get(0)).contains(peers[0]));
		assertTrue(Tasks.statiForPeer(list.get(0), peers[0]).contains(BCMessageStatus.EXECUTING_TASK));

		procInfo.updateTaskExecutionStatus(list.get(0).id(), TaskResult.newInstance().sender(peers[0]).status(BCMessageStatus.FINISHED_TASK),
				job.maxNrOfFinishedWorkersPerTask());
		assertTrue(Tasks.allAssignedPeers(list.get(0)).contains(peers[0]));
		assertTrue(Tasks.statiForPeer(list.get(0), peers[0]).contains(BCMessageStatus.FINISHED_TASK));

		procInfo.updateTaskExecutionStatus(list.get(1).id(), TaskResult.newInstance().sender(peers[1]).status(BCMessageStatus.EXECUTING_TASK),
				job.maxNrOfFinishedWorkersPerTask());
		assertTrue(Tasks.allAssignedPeers(list.get(1)).contains(peers[1]));
		assertTrue(Tasks.statiForPeer(list.get(1), peers[1]).contains(BCMessageStatus.EXECUTING_TASK));

		procInfo.updateTaskExecutionStatus(list.get(1).id(), TaskResult.newInstance().sender(peers[1]).status(BCMessageStatus.FINISHED_TASK),
				job.maxNrOfFinishedWorkersPerTask());
		assertTrue(Tasks.allAssignedPeers(list.get(1)).contains(peers[1]));
		assertTrue(Tasks.statiForPeer(list.get(1), peers[1]).contains(BCMessageStatus.FINISHED_TASK));

		for (Task task : list) { 
			ListMultimap<PeerAddress, BCMessageStatus> executingPeers = task.executingPeers();
			for (PeerAddress p : executingPeers.keySet()) {
				System.err.println(task.id()+": "+p + " " + executingPeers.get(p));
			}
		}

	}

}
