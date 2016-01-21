package mapreduce.execution.procedures;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.List;

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
		procedure.addOutputDomain(
				JobProcedureDomain.create("J1", 0, "E1", "WordCountMapper", 1).resultHash(Number160.ZERO));
		assertEquals(false, procedure.isFinished());
		assertEquals(1, procedure.nrOfOutputDomains());
		assertEquals(1, procedure.currentMaxNrOfSameResultHash().intValue());

		procedure.addOutputDomain(
				JobProcedureDomain.create("J1", 0, "E2", "WordCountMapper", 1).resultHash(Number160.ONE));
		assertEquals(false, procedure.isFinished());
		assertEquals(2, procedure.nrOfOutputDomains());
		assertEquals(1, procedure.currentMaxNrOfSameResultHash().intValue());

		procedure.addOutputDomain(
				JobProcedureDomain.create("J1", 0, "E3", "WordCountMapper", 1).resultHash(Number160.ONE));
		assertEquals(true, procedure.isFinished());
		assertEquals(3, procedure.nrOfOutputDomains());
		assertEquals(2, procedure.currentMaxNrOfSameResultHash().intValue());

		procedure.addOutputDomain(
				JobProcedureDomain.create("J1", 0, "E4", "WordCountMapper", 1).resultHash(Number160.ONE));
		assertEquals(true, procedure.isFinished());
		assertEquals(3, procedure.nrOfOutputDomains());
		assertEquals(2, procedure.currentMaxNrOfSameResultHash().intValue());
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

		// Only with the real input domain's tasksSize it is possible to determine how many tasks there have
		// to be
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
	public void testReset() throws Exception {
		Procedure procedure = Procedure.create(WordCountMapper.create(), 1).nrOfSameResultHash(1)
				.nrOfSameResultHashForTasks(10);
		ExecutorTaskDomain etd = ExecutorTaskDomain
				.create("", "3", 0, JobProcedureDomain.create("", 0, "3", "", 0)).resultHash(Number160.ONE);

		procedure.addTask(Task.create("1", "E1").addOutputDomain(etd));
		procedure.addTask(Task.create("2", "E1").addOutputDomain(etd));
		procedure.addTask(Task.create("3", "E1").addOutputDomain(etd));

		Field tasksF = Procedure.class.getDeclaredField("tasks");
		tasksF.setAccessible(true);
		@SuppressWarnings("unchecked")
		List<Task> tasks = (List<Task>) tasksF.get(procedure);
		for (Task task : tasks) {
			assertEquals(10, task.nrOfSameResultHash());
		}
		procedure.nrOfSameResultHashForTasks(1);
		for (Task task : tasks) {
			assertEquals(etd.executor(), task.resultOutputDomain().executor());
			assertEquals(Number160.ONE, task.resultOutputDomain().resultHash());
			assertEquals(1, task.nrOfOutputDomains());
			assertEquals(new Integer(1), task.currentMaxNrOfSameResultHash());
		}
		procedure.reset();
		assertEquals(3, procedure.tasksSize());
		for (Task task : tasks) {
			assertEquals(null, task.resultOutputDomain());
			assertEquals(0, task.nrOfOutputDomains());
			assertEquals(new Integer(0), task.currentMaxNrOfSameResultHash());
		}
	}

	@Test
	public void testNextExecutableTask() {
		int max = 25;
		Procedure procedure = Procedure.create(null, 0).nrOfSameResultHashForTasks(max / 2);
		procedure.addTask((Task) Task.create("hello", "E1").needsMultipleDifferentExecutors(false));
		procedure.addTask((Task) Task.create("world", "E1").needsMultipleDifferentExecutors(false));

		for (int i = 0; i < max; ++i) {
			Task task = procedure.nextExecutableTask();
			if (i >= 0 && i < (max / 2)) {
				assertEquals("hello", task.key());
			} else if (i >= (max / 2) && i < max - 1) {
				assertEquals("world", task.key());
			} else {
				assertEquals(null, task);
			}
		}
	}

	@Test
	public void testCalculateResultHash() throws Exception {
		// ===============================================================================
		// This method only returns a hash if all tasks finished and produced a hash. In any other case, null
		// is returned
		// ===============================================================================
		Procedure p = Procedure.create(Mockito.mock(IExecutable.class), 0);

		// If there is no dataInputDomain, it has to return null
		assertEquals(null, p.calculateResultHash());

		// If there is a dataInputDomain and the expected nr of files is larger than the number of tasks this
		// procedure currently has, there are still some tasks left to be retrieved and the method has to
		// return null immediatly
		Task task1 = Mockito.mock(Task.class);
		p.addTask(task1);
		JobProcedureDomain dataInputDomain = Mockito.mock(JobProcedureDomain.class);
		p.dataInputDomain(dataInputDomain);
		dataInputDomain.expectedNrOfFiles(2);
		assertEquals(null, p.calculateResultHash());

		// No tasks: Null!!
		dataInputDomain.expectedNrOfFiles(0);
		Field tasksInP = Procedure.class.getDeclaredField("tasks");
		tasksInP.setAccessible(true);
		((List<Task>) tasksInP.get(p)).clear();
		assertEquals(null, p.calculateResultHash());

		// Task not finished? null
		Task task2 = Mockito.mock(Task.class);
		Mockito.when(task1.isFinished()).thenReturn(false);
		Mockito.when(task2.isFinished()).thenReturn(false);
		p.addTask(task1);
		p.addTask(task2);
		assertEquals(null, p.calculateResultHash());
		Mockito.when(task1.isFinished()).thenReturn(true);
		assertEquals(null, p.calculateResultHash());
		Mockito.when(task2.isFinished()).thenReturn(true);

		// Although both tasks are finished, none return a result hash --> STILL NULL
		Mockito.when(task1.calculateResultHash()).thenReturn(null);
		Mockito.when(task1.calculateResultHash()).thenReturn(null);
		assertEquals(null, p.calculateResultHash());
		// Already one null but still nulls left... still null!
		Mockito.when(task1.calculateResultHash()).thenReturn(Number160.ONE);
		assertEquals(null, p.calculateResultHash());
		// Finally, both tasks are finished and return a result hash! Result is the xOR of all result hashs
		// with Number160.ZERO as start. Aso 0.xor(1) == 1, and 1.xor(1) == 0, the result should be
		// Number160.ZERO
		Mockito.when(task2.calculateResultHash()).thenReturn(Number160.ONE);
		assertEquals(Number160.ZERO, p.calculateResultHash());

	}

	@Test
	public void testIsCompleted() {

	}

	@Test
	public void testNrOfFinishedTasks() {

	}

	@Test
	public void testNrOfResultHashForTasks() {

	}

	@Test
	public void testAddOutputDomain() {

	}

	@Test
	public void testCurrentMaxNrOfSameResultHash() {

	}

	@Test
	public void testResultOutputDomainAndCheckIfFinished() {

	}

	@Test
	public void testContainsExecutor() {

	}
}
