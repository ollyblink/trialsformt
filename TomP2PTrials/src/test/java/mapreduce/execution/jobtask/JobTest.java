package mapreduce.execution.jobtask;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import mapreduce.execution.job.Job;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;

public class JobTest {

	@Test
	public void testAddingAndIncrementProcedure() {

		Job job = Job.create("TEST").addSucceedingProcedure(WordCountMapper.create()).addSucceedingProcedure(WordCountReducer.create());

		assertEquals("StartProcedure", job.procedure(-100).executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(-10).executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(-1).executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(0).executable().getClass().getSimpleName());

		assertEquals("StartProcedure", job.procedure(0).executable().getClass().getSimpleName());
		assertEquals("WordCountMapper", job.procedure(1).executable().getClass().getSimpleName());
		assertEquals("WordCountReducer", job.procedure(2).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(3).executable().getClass().getSimpleName());

		assertEquals("EndProcedure", job.procedure(10).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(100).executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(1000).executable().getClass().getSimpleName());

		assertEquals("StartProcedure", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("StartProcedure", job.procedure(job.currentProcedure().procedureIndex()).executable().getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("WordCountMapper", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("WordCountMapper", job.procedure(job.currentProcedure().procedureIndex()).executable().getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("WordCountReducer", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("WordCountReducer", job.procedure(job.currentProcedure().procedureIndex()).executable().getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("EndProcedure", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(job.currentProcedure().procedureIndex()).executable().getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("EndProcedure", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(job.currentProcedure().procedureIndex()).executable().getClass().getSimpleName());

		job.incrementProcedureIndex();
		assertEquals("EndProcedure", job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals("EndProcedure", job.procedure(job.currentProcedure().procedureIndex()).executable().getClass().getSimpleName());

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
