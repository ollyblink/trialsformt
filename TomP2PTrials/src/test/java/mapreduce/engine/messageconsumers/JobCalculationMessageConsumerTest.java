package mapreduce.engine.messageconsumers;

import static org.junit.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.storage.IDHTConnectionProvider;

public class JobCalculationMessageConsumerTest {

	private static JobCalculationMessageConsumer calculationMsgConsumer;
	private static IDHTConnectionProvider mockDHT;
	private static JobCalculationExecutor calculationExecutor;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		mockDHT = Mockito.mock(IDHTConnectionProvider.class);
		// Calculation Executor
		calculationExecutor = Mockito.mock(JobCalculationExecutor.class);
		Mockito.when(calculationExecutor.id()).thenReturn("E1");
		// Calculation MessageConsumer
		calculationMsgConsumer = JobCalculationMessageConsumer.create();
		calculationMsgConsumer.dhtConnectionProvider(mockDHT).executor(calculationExecutor);

	}

	@Test
	public void testTryIncrementProcedure() throws Exception {

		Method tryIncrementProcedureMethod = calculationMsgConsumer.getClass().getDeclaredMethod("tryIncrementProcedure", Job.class,
				JobProcedureDomain.class, JobProcedureDomain.class);
		tryIncrementProcedureMethod.setAccessible(true);

		Job job = Job.create("S1")
				.addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false);
		JobProcedureDomain firstInputDomain = JobProcedureDomain.create(job.id(), 0, "E1", "INITIAL", -1);
		JobProcedureDomain secondInputDomain = JobProcedureDomain.create(job.id(), 0, "E1", StartProcedure.class.getSimpleName(), 0);
		JobProcedureDomain thirdInputDomain = JobProcedureDomain.create(job.id(), 0, "E1", WordCountMapper.class.getSimpleName(), 1);
		JobProcedureDomain lastInputDomain = JobProcedureDomain.create(job.id(), 0, "E1", EndProcedure.class.getSimpleName(), 2);

		JobProcedureDomain firstOutputDomain = secondInputDomain;
		JobProcedureDomain secondOutputDomain = thirdInputDomain;
		JobProcedureDomain thirdOutputDomain = lastInputDomain;

		job.currentProcedure().dataInputDomain(firstInputDomain);
		job.currentProcedure().addOutputDomain(firstOutputDomain);

		// Currently: second procedure (Start procedure)
		job.incrementProcedureIndex(); 

		// Incoming: result for procedure before: nothing happens
		String procedureNameBefore = job.currentProcedure().executable().getClass().getSimpleName();
		int procedureIndexBefore = job.currentProcedure().procedureIndex();
		tryIncrementProcedureMethod.invoke(calculationMsgConsumer, job, firstInputDomain, firstOutputDomain);
		String procedureNameAfter = job.currentProcedure().executable().getClass().getSimpleName();
		int procedureIndexAfter = job.currentProcedure().procedureIndex();
		assertEquals(procedureNameBefore, procedureNameAfter);
		assertEquals(procedureIndexBefore, procedureIndexAfter);
		assertEquals(secondInputDomain, job.currentProcedure().dataInputDomain());

		// Incoming: result for same procedure: nothing happens
		procedureNameBefore = job.currentProcedure().executable().getClass().getSimpleName();
		procedureIndexBefore = job.currentProcedure().procedureIndex();
		tryIncrementProcedureMethod.invoke(calculationMsgConsumer, job, secondInputDomain, secondOutputDomain);
		procedureNameAfter = job.currentProcedure().executable().getClass().getSimpleName();
		procedureIndexAfter = job.currentProcedure().procedureIndex();
		assertEquals(procedureNameBefore, procedureNameAfter);
		assertEquals(procedureIndexBefore, procedureIndexAfter);
		assertEquals(secondInputDomain, job.currentProcedure().dataInputDomain());

		// //Incoming: after: updates current procedure to the one received
		procedureNameBefore = job.currentProcedure().executable().getClass().getSimpleName();
		procedureIndexBefore = job.currentProcedure().procedureIndex();
		tryIncrementProcedureMethod.invoke(calculationMsgConsumer, job, thirdInputDomain, thirdOutputDomain);
		procedureNameAfter = job.currentProcedure().executable().getClass().getSimpleName();
		procedureIndexAfter = job.currentProcedure().procedureIndex();
		assertEquals(EndProcedure.class.getSimpleName(), procedureNameAfter);
		assertEquals(2, procedureIndexAfter);
		assertEquals(thirdInputDomain, job.currentProcedure().dataInputDomain());
	}

	@Test
	public void testCancelTaskExecution() {
	}

	@Test
	public void testHandleCompletedProcedure() {
	}

	@Test
	public void testCancelProcedureExecution() {
	}
}
