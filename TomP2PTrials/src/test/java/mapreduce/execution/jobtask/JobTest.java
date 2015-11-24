package mapreduce.execution.jobtask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;
import mapreduce.execution.computation.standardprocedures.NullMapReduceProcedure;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class JobTest {

	@Test
	public void testNextProcedure() {
		BlockingQueue<Task> tasksForProcedure = new LinkedBlockingQueue<Task>();
		tasksForProcedure.add(Task.newTask());
		tasksForProcedure.add(Task.newTask());
		tasksForProcedure.add(Task.newTask());
		tasksForProcedure.add(Task.newTask());
		Job job = Job.newJob().maxNrOfFinishedPeers(3).nextProcedure(new WordCountMapper(), tasksForProcedure);

		BlockingQueue<Task> tasks = job.tasksFor(job.nextProcedure());
		assertFalse(tasks == null);
		assertEquals(4, tasks.size());
		assertEquals("WordCountMapper", job.nextProcedure().toString());

		BlockingQueue<Task> tasksForProcedure2 = new LinkedBlockingQueue<Task>();
		tasksForProcedure2.add(Task.newTask());
		tasksForProcedure2.add(Task.newTask());
		job.nextProcedure(new NullMapReduceProcedure(), tasksForProcedure2);

		tasks = job.tasksFor(job.nextProcedure());
		assertFalse(tasks == null);
		assertEquals(2, tasks.size());
		assertEquals("NullMapReduceProcedure", job.nextProcedure().toString());
	}

	@Test
	public void testUpdateTaskStati() {
		BlockingQueue<Task> tasksForProcedure = new LinkedBlockingQueue<Task>();
		tasksForProcedure.add(Task.newTask().id("TEST_TASK_1"));
		tasksForProcedure.add(Task.newTask().id("TEST_TASK_2"));
		tasksForProcedure.add(Task.newTask().id("TEST_TASK_3"));
		tasksForProcedure.add(Task.newTask().id("TEST_TASK_4"));
		Job job = Job.newJob().maxNrOfFinishedPeers(3).nextProcedure(new WordCountMapper(), tasksForProcedure);
		ArrayList<Task> list = new ArrayList<Task>(tasksForProcedure);
		PeerAddress[] peers = new PeerAddress[3];
		peers[0] = new PeerAddress(Number160.createHash("1"));
		peers[1] = new PeerAddress(Number160.createHash("2")); 
		
		job.updateTaskStatus("TEST_TASK_1", peers[0], JobStatus.EXECUTING_TASK); 
		assertTrue(list.get(0).allAssignedPeers().contains(peers[0]));
		assertTrue(list.get(0).statiForPeer(peers[0]).contains(JobStatus.EXECUTING_TASK));

		job.updateTaskStatus("TEST_TASK_1", peers[0], JobStatus.FINISHED_TASK); 
		assertTrue(list.get(0).allAssignedPeers().contains(peers[0]));
		assertTrue(list.get(0).statiForPeer(peers[0]).contains(JobStatus.FINISHED_TASK)); 

		job.updateTaskStatus("TEST_TASK_1", peers[1], JobStatus.EXECUTING_TASK); 
		assertTrue(list.get(0).allAssignedPeers().contains(peers[1]));
		assertTrue(list.get(0).statiForPeer(peers[1]).contains(JobStatus.EXECUTING_TASK));

		job.updateTaskStatus("TEST_TASK_1", peers[1], JobStatus.FINISHED_TASK); 
		assertTrue(list.get(0).allAssignedPeers().contains(peers[1]));
		assertTrue(list.get(0).statiForPeer(peers[1]).contains(JobStatus.FINISHED_TASK)); 
		
	}

}