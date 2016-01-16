package mapreduce.execution.procedures;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.tasks.Task;
import net.tomp2p.peers.Number160;

public class ProcedureTest {

	@Test
	public void testNrOfSameResultHash() {
		Procedure procedure = Procedure.create(WordCountMapper.create(), 1);
		procedure.nrOfSameResultHash(2);
		procedure.addOutputDomain(JobProcedureDomain.create(null, null, null, -1).resultHash(Number160.ZERO));
		assertEquals(false, procedure.isFinished());
		assertEquals(1, procedure.nrOfOutputDomains());

		procedure.addOutputDomain(JobProcedureDomain.create(null, null, null, -1).resultHash(Number160.ONE));
		assertEquals(false, procedure.isFinished());
		assertEquals(2, procedure.nrOfOutputDomains());

		procedure.addOutputDomain(JobProcedureDomain.create(null, null, null, -1).resultHash(Number160.ONE));
		assertEquals(true, procedure.isFinished());
		assertEquals(3, procedure.nrOfOutputDomains());

		// Cannot add more output procedure domains once it finished
		procedure.addOutputDomain(JobProcedureDomain.create(null, null, null, -1).resultHash(Number160.ONE));
		assertEquals(true, procedure.isFinished());
		assertEquals(3, procedure.nrOfOutputDomains());
	}

	@Test
	public void calculateOverallResultHash() {
		Procedure procedure = Procedure.create(WordCountMapper.create(), 1);
		Task task1 = Mockito.mock(Task.class);
		Mockito.when(task1.calculateResultHash()).thenReturn(Number160.ZERO);
		Mockito.when(task1.isFinished()).thenReturn(true);

		Task task2 = Mockito.mock(Task.class);
		Mockito.when(task2.calculateResultHash()).thenReturn(Number160.ZERO);
		Mockito.when(task2.isFinished()).thenReturn(true);

		Task task3 = Mockito.mock(Task.class);
		Mockito.when(task3.calculateResultHash()).thenReturn(Number160.ONE);
		Mockito.when(task3.isFinished()).thenReturn(true);

		Task task4 = Mockito.mock(Task.class);
		Mockito.when(task4.calculateResultHash()).thenReturn(Number160.ONE);
		Mockito.when(task4.isFinished()).thenReturn(true);

		// Only with the real input domain's tasksSize it is possible to determine how many tasks there have to be
		JobProcedureDomain dataInputDomain = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(dataInputDomain.expectedNrOfFiles()).thenReturn(4);
		procedure.dataInputDomain(dataInputDomain);

		assertEquals(null, procedure.calculateResultHash());
		assertEquals(0, procedure.nrOfFinishedTasks());

		procedure.addTask(task1);
		assertEquals(null, procedure.calculateResultHash());
		assertEquals(1, procedure.nrOfFinishedTasks());

		procedure.addTask(task2);
		assertEquals(null, procedure.calculateResultHash());
		assertEquals(2, procedure.nrOfFinishedTasks());

		procedure.addTask(task3);
		assertEquals(null, procedure.calculateResultHash());
		assertEquals(3, procedure.nrOfFinishedTasks());

		procedure.addTask(task4);
		assertEquals(Number160.ZERO, procedure.calculateResultHash());
		assertEquals(4, procedure.nrOfFinishedTasks());

		// Check with unfinished task
		Mockito.when(dataInputDomain.expectedNrOfFiles()).thenReturn(5);
		Task task5 = Mockito.mock(Task.class);
		Mockito.when(task5.calculateResultHash()).thenReturn(Number160.ONE);
		Mockito.when(task5.isFinished()).thenReturn(false);
		procedure.addTask(task5);
		assertEquals(null, procedure.calculateResultHash());
		assertEquals(4, procedure.nrOfFinishedTasks());

		Mockito.when(task5.isFinished()).thenReturn(true);
		assertEquals(Number160.ONE, procedure.calculateResultHash());
		assertEquals(5, procedure.nrOfFinishedTasks());

	}

	@Test
	public void testReset() {
		Procedure procedure = Procedure.create(WordCountMapper.create(), 1).nrOfSameResultHash(1).nrOfSameResultHashForTasks(10);
		ExecutorTaskDomain etd = ExecutorTaskDomain.create("", "3", 0, JobProcedureDomain.create("", "3", "", 0)).resultHash(Number160.ONE); 

		procedure.addTask(Task.create("1").addOutputDomain(etd));
		procedure.addTask(Task.create("2").addOutputDomain(etd));
		procedure.addTask(Task.create("3").addOutputDomain(etd));
		for (Task task : procedure.tasks()) {
			assertEquals(10, task.nrOfSameResultHash());
		}
		procedure.nrOfSameResultHashForTasks(1);
		for (Task task : procedure.tasks()) {
			assertEquals(etd.executor(), task.resultOutputDomain().executor());
			assertEquals(Number160.ONE, task.resultOutputDomain().resultHash());
			assertEquals(1, task.nrOfOutputDomains());
			assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		}
		procedure.reset();
		assertEquals(3, procedure.tasks().size());
		for (Task task : procedure.tasks()) {
			assertEquals(null, task.resultOutputDomain());
			assertEquals(0, task.nrOfOutputDomains());
			assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		}
	}
	

}
