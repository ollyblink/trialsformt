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

		// Currently: first procedure (Start procedure)
		Job job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false);
		JobProcedureDomain dataInputDomain = JobProcedureDomain.create(job.id(), 0, "E1", "INITIAL", -1);
		job.currentProcedure().dataInputDomain(dataInputDomain);
		JobProcedureDomain rOutputDomain = JobProcedureDomain.create(job.id(), 0, "E1",
				job.currentProcedure().executable().getClass().getSimpleName(), job.currentProcedure().procedureIndex());
		job.currentProcedure().addOutputDomain(rOutputDomain);

		
		// Incoming: same: nothing happens 
		String procedureNameBefore = job.currentProcedure().executable().getClass().getSimpleName();
		int procedureIndexBefore = job.currentProcedure().procedureIndex();
		tryIncrementProcedureMethod.invoke(calculationMsgConsumer, job, dataInputDomain, rOutputDomain);
		String procedureNameAfter = job.currentProcedure().executable().getClass().getSimpleName();
		int procedureIndexAfter = job.currentProcedure().procedureIndex();
		assertEquals(procedureNameBefore, procedureNameAfter);
		assertEquals(procedureIndexBefore, procedureIndexAfter);
		
//		//Incoming: after: updates!
//		String procedureNameBefore = job.currentProcedure().executable().getClass().getSimpleName();
//		int procedureIndexBefore = job.currentProcedure().procedureIndex();
//		tryIncrementProcedureMethod.invoke(calculationMsgConsumer, job, dataInputDomain, rOutputDomain);
//		String procedureNameAfter = job.currentProcedure().executable().getClass().getSimpleName();
//		int procedureIndexAfter = job.currentProcedure().procedureIndex();
//		assertEquals(procedureNameBefore, procedureNameAfter);
//		assertEquals(procedureIndexBefore, procedureIndexAfter);
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
