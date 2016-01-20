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
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;

public class ProcedureUpdateTest {

	private JobCalculationExecutor calculationExecutor;
	private JobCalculationMessageConsumer calculationMsgConsumer;
	private Job job;

	@Before
	public void setUpBeforeTest() throws Exception {

		// Calculation Executor
		calculationExecutor = Mockito.mock(JobCalculationExecutor.class);
		// Calculation MessageConsumer
		calculationMsgConsumer = Mockito.mock(JobCalculationMessageConsumer.class);
		Mockito.when(calculationMsgConsumer.executor()).thenReturn(calculationExecutor);
		job = Job.create("S1");
		procedureUpdate = new ProcedureUpdate(job, calculationMsgConsumer);
	}

	private IDomain outputDomain;
	private Procedure procedure;
	private ProcedureUpdate procedureUpdate;

	@Test
	public void testBothNull() {
		// Test if any null
		// Both null
		outputDomain = null;
		procedure = null;
		procedureUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(null, procedure);

	}

	@Test
	public void testProcedureNull() {
		// Procedure null
		procedure = null;
		outputDomain = Mockito.mock(IDomain.class);
		procedureUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(null, procedure);
	}

	@Test
	public void testDomainNull() {
		// Output domain null
		Mockito.mock(Procedure.class);
		Procedure tmp = procedure;
		outputDomain = null;
		procedureUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(tmp, procedure);

	}

	@Test
	public void testNonNull() {
		// Both not null
		Mockito.mock(Procedure.class);
		Procedure tmp = procedure;
		outputDomain = Mockito.mock(IDomain.class);
		procedureUpdate.executeUpdate(outputDomain, procedure);
		assertEquals(tmp, procedure);
	}

	@Test(expected = ClassCastException.class)
	public void testWrongDomainTypeExceptionCaught() {
		IDomain outputDomain = Mockito.mock(ExecutorTaskDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		procedureUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
	}

	@Test(expected = NullPointerException.class)
	public void testMessageConsumerNullException() {
		procedureUpdate = new ProcedureUpdate(job, null);
		IDomain outputDomain = Mockito.mock(JobProcedureDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.isFinished()).thenReturn(true);
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		procedureUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
	}

	@Test(expected = NullPointerException.class)
	public void testJobNullException() {
		procedureUpdate = new ProcedureUpdate(null, calculationMsgConsumer);
		IDomain outputDomain = Mockito.mock(JobProcedureDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.isFinished()).thenReturn(true);
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		procedureUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
	}

	@Test(expected = NullPointerException.class)
	public void testBothVarsNull() {
		procedureUpdate = new ProcedureUpdate(null, null);
		IDomain outputDomain = Mockito.mock(JobProcedureDomain.class);
		Procedure procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.isFinished()).thenReturn(true);
		Procedure pTmp = procedure;
		// Wrong domain type --> returns old procedure and logs exception
		procedureUpdate.internalUpdate(outputDomain, procedure);
		assertEquals(pTmp, procedure);
	}

	@Test
	public void testFinishProcedure() {
		// Here the procedure is tried to be finished and the next procedure to be executed
		// One procedure is needed since else it would directly jump to EndProcedure and finish the job
		// Simplest possible idea: procedure only needs to be executed once
		job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false);
		procedureUpdate = new ProcedureUpdate(job, calculationMsgConsumer);

		Procedure start = job.currentProcedure();
		assertEquals(start, job.currentProcedure());
		assertEquals(false, start.isFinished());
		assertEquals(0, job.currentProcedure().procedureIndex());
		assertEquals(StartProcedure.class.getSimpleName(),
				job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(false, job.isFinished());
		assertEquals(null, job.currentProcedure().dataInputDomain());
		Mockito.verify(calculationMsgConsumer, Mockito.times(0))
				.cancelProcedureExecution(start.dataInputDomain().toString());

		IDomain o = JobProcedureDomain.create(job.id(), 0, "E1", start.getClass().getSimpleName(),
				start.procedureIndex());

		procedureUpdate.internalUpdate(o, job.currentProcedure());
		Procedure p = job.currentProcedure();
		assertEquals(true, start.isFinished());
		assertEquals(false, p.isFinished());
		assertEquals(1, job.currentProcedure().procedureIndex());
		assertEquals(WordCountMapper.class.getSimpleName(),
				job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(false, job.isFinished());
		assertEquals(o, job.currentProcedure().dataInputDomain());
		Mockito.verify(calculationMsgConsumer, Mockito.times(1))
				.cancelProcedureExecution(start.dataInputDomain().toString());

		// Finish the job
		o = JobProcedureDomain.create(job.id(), 0, "E1", p.getClass().getSimpleName(), p.procedureIndex());
		Procedure beforeInc = p;
		procedureUpdate.internalUpdate(o, job.currentProcedure());
		p = job.currentProcedure();

		assertEquals(p.executable().getClass().getSimpleName(),
				job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(true, beforeInc.isFinished());
		assertEquals(true, p.isFinished());
		assertEquals(2, job.currentProcedure().procedureIndex());
		assertEquals(EndProcedure.class.getSimpleName(),
				job.currentProcedure().executable().getClass().getSimpleName());
		assertEquals(EndProcedure.class.getSimpleName(), p.executable().getClass().getSimpleName());
		assertEquals(true, job.isFinished());
		assertEquals(o, job.currentProcedure().dataInputDomain());
		Mockito.verify(calculationMsgConsumer, Mockito.times(1))
				.cancelProcedureExecution(start.dataInputDomain().toString());
		Mockito.verify(calculationMsgConsumer, Mockito.times(1))
				.cancelProcedureExecution(beforeInc.dataInputDomain().toString());
	}

}
