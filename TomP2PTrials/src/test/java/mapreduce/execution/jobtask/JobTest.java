package mapreduce.execution.jobtask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.omg.CORBA.PRIVATE_MEMBER;

import com.google.common.collect.ListMultimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.procedures.NullMapReduceProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.Tasks;
import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;

public class JobTest {

	@Test
	public void testAddingProcedure() {

		Job job = Job.create("TEST", PriorityLevel.MODERATE).addSubsequentProcedure(WordCountMapper.create())
				.addSubsequentProcedure(WordCountReducer.create());

		assertEquals("StartProcedure", job.procedure(-100).executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(-10).executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(-1).executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(0).executable().getClass().getSimpleName());

		assertEquals("StartProcedure", job.procedure(0).executable().getClass().getSimpleName());
		assertEquals("WordCountMapper", job.procedure(1).executable().getClass().getSimpleName());
		assertEquals("WordCountReducer", job.procedure(2).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(3).executable().getClass().getSimpleName());

		assertEquals("StartProcedure", job.previousProcedure().executable().getClass().getSimpleName());
		assertEquals("WordCountMapper", job.currentProcedure().executable().getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("StartProcedure", job.procedure(0).executable().getClass().getSimpleName());
		assertEquals("WordCountMapper", job.procedure(1).executable().getClass().getSimpleName());
		assertEquals("WordCountReducer", job.procedure(2).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(3).executable().getClass().getSimpleName());

		assertEquals("WordCountMapper", job.previousProcedure().executable().getClass().getSimpleName());
		assertEquals("WordCountReducer", job.currentProcedure().executable().getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("StartProcedure", job.procedure(0).executable().getClass().getSimpleName());
		assertEquals("WordCountMapper", job.procedure(1).executable().getClass().getSimpleName());
		assertEquals("WordCountReducer", job.procedure(2).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(3).executable().getClass().getSimpleName());

		assertEquals("WordCountReducer", job.previousProcedure().executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.currentProcedure().executable().getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("StartProcedure", job.procedure(0).executable().getClass().getSimpleName());
		assertEquals("WordCountMapper", job.procedure(1).executable().getClass().getSimpleName());
		assertEquals("WordCountReducer", job.procedure(2).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(3).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(4).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(5).executable().getClass().getSimpleName());

		assertEquals("EndProcedure", job.previousProcedure().executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.currentProcedure().executable().getClass().getSimpleName());
	}

	@Test
	public void testUpdateTaskStati() {
		int counter = 0;

		Job job = Job.create("TEST", PriorityLevel.MODERATE).maxNrOfFinishedWorkersPerTask(3)
				.addSubsequentProcedure(NullMapReduceProcedure.newInstance());
		List<Task> list = new ArrayList<Task>();
		list.add(Task.create("word" + (counter++), job.previousProcedure().jobProcedureDomain()));
		list.add(Task.create("word" + (counter++), job.previousProcedure().jobProcedureDomain()));
		job.previousProcedure().tasks(list);

		String[] peers = new String[3];
		peers[0] = "Executor_1";
		peers[1] = "Executor_3";

		Tasks.updateStati(list.get(0), TaskResult.create().sender(peers[0]).status(BCMessageStatus.EXECUTING_TASK),
				job.maxNrOfFinishedWorkersPerTask());
		assertTrue(Tasks.allAssignedPeers(list.get(0)).contains(peers[0]));
		assertTrue(Tasks.statiForPeer(list.get(0), peers[0]).contains(BCMessageStatus.EXECUTING_TASK));

		Tasks.updateStati(list.get(0), TaskResult.create().sender(peers[0]).status(BCMessageStatus.FINISHED_TASK),
				job.maxNrOfFinishedWorkersPerTask());
		assertTrue(Tasks.allAssignedPeers(list.get(0)).contains(peers[0]));
		assertTrue(Tasks.statiForPeer(list.get(0), peers[0]).contains(BCMessageStatus.FINISHED_TASK));

		Tasks.updateStati(list.get(1), TaskResult.create().sender(peers[1]).status(BCMessageStatus.EXECUTING_TASK),
				job.maxNrOfFinishedWorkersPerTask());
		assertTrue(Tasks.allAssignedPeers(list.get(1)).contains(peers[1]));
		assertTrue(Tasks.statiForPeer(list.get(1), peers[1]).contains(BCMessageStatus.EXECUTING_TASK));

		Tasks.updateStati(list.get(1), TaskResult.create().sender(peers[1]).status(BCMessageStatus.FINISHED_TASK),
				job.maxNrOfFinishedWorkersPerTask());
		assertTrue(Tasks.allAssignedPeers(list.get(1)).contains(peers[1]));
		assertTrue(Tasks.statiForPeer(list.get(1), peers[1]).contains(BCMessageStatus.FINISHED_TASK));

		for (Task task : list) {
			ListMultimap<String, BCMessageStatus> executingPeers = task.executingPeers();
			for (String p : executingPeers.keySet()) {
				System.err.println(task.id() + ": " + p + " " + executingPeers.get(p));
			}
		}

	}

	@Test
	public void testJobComparison() {
		List<Job> jobs = new ArrayList<>();
		for (int i = 0; i < 10; ++i) {
			PriorityLevel level = (i % 3 == 0 ? PriorityLevel.LOW : (i % 2 == 0 ? PriorityLevel.MODERATE : PriorityLevel.HIGH));
			jobs.add(Job.create("TEST", level));
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(jobs);
//		for (int i = 0; i < 10; ++i) {
//			System.err.println(jobs.get(i).priorityLevel() + ", " + jobs.get(i).creationTime() + ", " + jobs.get(i).id());
//		}
		for (int i = 0; i < 10; ++i) {
			if (i >= 0 && i < 3) {
				assertEquals(PriorityLevel.HIGH, jobs.get(i).priorityLevel());
				if (i < 2) {
					assertEquals(true, jobs.get(i).creationTime() < jobs.get(i + 1).creationTime());
				}
			}
			if (i >= 3 && i < 6) {
				assertEquals(PriorityLevel.MODERATE, jobs.get(i).priorityLevel());
				if (i < 5) {
					assertEquals(true, jobs.get(i).creationTime() < jobs.get(i + 1).creationTime());
				}
			}
			if (i >= 6 && i < 10) {
				assertEquals(PriorityLevel.LOW, jobs.get(i).priorityLevel());
				if (i < 9) {
					assertEquals(true, jobs.get(i).creationTime() < jobs.get(i + 1).creationTime());
				}
			}
		}

	}

}
