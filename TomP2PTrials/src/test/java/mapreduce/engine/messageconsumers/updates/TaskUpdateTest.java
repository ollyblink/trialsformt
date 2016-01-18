package mapreduce.engine.messageconsumers.updates;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.tasks.Task;

public class TaskUpdateTest {
	private TaskUpdate taskUpdate;
	private JobCalculationMessageConsumer calculationMsgConsumer;
	private JobCalculationExecutor calculationExecutor;

	private IDomain outputDomain;
	private Procedure procedure;

	@Before
	public void setUpBeforeTest() throws Exception {
		// Calculation Executor
		calculationExecutor = Mockito.mock(JobCalculationExecutor.class);
		// Calculation MessageConsumer
		calculationMsgConsumer = Mockito.mock(JobCalculationMessageConsumer.class);
		Mockito.when(calculationMsgConsumer.executor()).thenReturn(calculationExecutor);

		// Actual update
		taskUpdate = new TaskUpdate(calculationMsgConsumer);
	}

	@Test
	public void testBothNull() {
		// Test if any null
		// Both null
		outputDomain = null;
		procedure = null;
		taskUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(null, procedure);

	}

	@Test
	public void testProcedureNull() {
		// Procedure null
		procedure = null;
		outputDomain = Mockito.mock(IDomain.class);
		taskUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(null, procedure);
	}

	@Test
	public void testDomainNull() {
		// Output domain null
		procedure = Mockito.mock(Procedure.class);
		Procedure tmp = procedure;
		outputDomain = null;
		taskUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(tmp, procedure);

	}

	@Test
	public void testNonNull() {
		// Both not null
		procedure = Mockito.mock(Procedure.class);
		Procedure tmp = procedure;
		outputDomain = Mockito.mock(IDomain.class);
		taskUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(tmp, procedure);
	}

	@Test(expected = ClassCastException.class)
	public void testWrongDomainTypeExceptionCaught() throws ClassCastException, NullPointerException {
		IDomain outputDomain = Mockito.mock(JobProcedureDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		taskUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
	}

	@Test(expected = NullPointerException.class)
	public void testMessageConsumerNullExceptionCaught() throws ClassCastException, NullPointerException {
		TaskUpdate tmp = taskUpdate;
		taskUpdate = new TaskUpdate(null);
		IDomain outputDomain = Mockito.mock(ExecutorTaskDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		taskUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
		taskUpdate = tmp;
	}

	@Test
	public void testTaskUpdateUnfinished() throws ClassCastException, NullPointerException {
		// In this case, procedure does not have any tasks --> task will be added to procedure
		// Task will not be finished as it needs two executions to be marked finished
		Procedure procedure = Procedure.create(WordCountMapper.class, 1).nrOfSameResultHashForTasks(2).needsMultipleDifferentExecutorsForTasks(true);
		ExecutorTaskDomain outputDomain = ExecutorTaskDomain.create("hello", "E1", 0, null);

		assertEquals(0, procedure.tasks().size());
		taskUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(1, procedure.tasks().size());
		Task task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(1, task.nrOfOutputDomains());
		assertEquals(false, task.isFinished());
	}

	@Test
	public void testTaskUpdateSameInputDomainFinished() throws ClassCastException, NullPointerException {
		// In this case, procedure does not have any tasks --> task will be added to procedure
		// Task will be finished as after second execution
		Procedure procedure = Procedure.create(WordCountMapper.class, 1).nrOfSameResultHashForTasks(2).needsMultipleDifferentExecutorsForTasks(false);
		ExecutorTaskDomain outputDomain = ExecutorTaskDomain.create("hello", "E1", 0, null);

		assertEquals(0, procedure.tasks().size());
		taskUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(1, procedure.tasks().size());
		Task task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		taskUpdate.internalUpdate(outputDomain, procedure);// Second execution with the same output domain should not have any effect
		assertEquals(1, procedure.tasks().size());
		task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From same executor, but different domain (it's an additional execution!!)
		ExecutorTaskDomain outputDomain2 = ExecutorTaskDomain.create("hello", "E1", 1, null);
		taskUpdate.internalUpdate(outputDomain2, procedure);
		assertEquals(1, procedure.tasks().size()); // will stay the same as it contains it already
		task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(true, task.isFinished());
		assertEquals(2, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(1)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From same executor, but different domain (it's an additional execution!!)
		ExecutorTaskDomain outputDomain3 = ExecutorTaskDomain.create("hello", "E1", 2, null);
		taskUpdate.internalUpdate(outputDomain3, procedure);
		assertEquals(1, procedure.tasks().size()); // will stay the same as it contains it already
		task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(true, task.isFinished());

		// Nothing changes as this task is already finished
		assertEquals(2, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(1)).switchDataFromTaskToProcedureDomain(procedure, task);
	}

	@Test
	public void testTaskUpdateDifferentInputDomainFinished() throws ClassCastException, NullPointerException {
		// In this case, procedure does not have any tasks --> task will be added to procedure
		// Task will be finished as after second execution
		Procedure procedure = Procedure.create(WordCountMapper.class, 1).nrOfSameResultHashForTasks(2).needsMultipleDifferentExecutorsForTasks(true);
		ExecutorTaskDomain outputDomain = ExecutorTaskDomain.create("hello", "E1", 0, null);

		assertEquals(0, procedure.tasks().size());
		taskUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(1, procedure.tasks().size());
		Task task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		taskUpdate.internalUpdate(outputDomain, procedure);// Second execution with the same output domain should not have any effect
		assertEquals(1, procedure.tasks().size());
		task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(1, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From same executor, but different domain (it's an additional execution!!)
		ExecutorTaskDomain outputDomain2 = ExecutorTaskDomain.create("hello", "E1", 1, null);
		taskUpdate.internalUpdate(outputDomain2, procedure);
		assertEquals(1, procedure.tasks().size()); // will stay the same as it contains it already
		task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(2, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From same executor, but different domain (it's an additional execution!!)
		ExecutorTaskDomain outputDomain3 = ExecutorTaskDomain.create("hello", "E1", 2, null);
		taskUpdate.internalUpdate(outputDomain3, procedure);
		assertEquals(1, procedure.tasks().size()); // will stay the same as it contains it already
		task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(false, task.isFinished());
		assertEquals(3, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(0)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(0)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From different executor finally
		ExecutorTaskDomain outputDomain4 = ExecutorTaskDomain.create("hello", "E2", 0, null);
		taskUpdate.internalUpdate(outputDomain4, procedure);
		assertEquals(1, procedure.tasks().size()); // will stay the same as it contains it already
		task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(true, task.isFinished());
		assertEquals(4, task.nrOfOutputDomains());
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(1)).switchDataFromTaskToProcedureDomain(procedure, task);

		// From different executor again, but nothing changes anymore due to the finished procedure
		ExecutorTaskDomain outputDomain5 = ExecutorTaskDomain.create("hello", "E3", 0, null);
		taskUpdate.internalUpdate(outputDomain5, procedure);
		assertEquals(1, procedure.tasks().size()); // will stay the same as it contains it already
		task = procedure.tasks().get(0);
		assertEquals("hello", task.key());
		assertEquals(true, task.isFinished());
		assertEquals(4, task.nrOfOutputDomains()); // Won't be added anymore
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).cancelTaskExecution(procedure, task);
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(1)).switchDataFromTaskToProcedureDomain(procedure, task);
	}

}
