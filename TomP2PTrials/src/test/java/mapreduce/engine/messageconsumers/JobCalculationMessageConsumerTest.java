package mapreduce.engine.messageconsumers;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;

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
//		calculationMsgConsumer.cancelProcedureExecution(procedure);
//		calculationMsgConsumer.cancelTaskExecution(procedure, task);
//		calculationMsgConsumer.handleCompletedProcedure(job, outputDomain, inputDomain);
//		calculationMsgConsumer.handleCompletedTask(job, outputDomain, inputDomain);
//		calculationMsgConsumer.printResults(job);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testHandleCompletedTask() {
	}

	@Test
	public void testCancelTaskExecution() {
	}

	@Test
	public void testHandleCompletedProcedure() {
	}

	@Test
	public void testcancelProcedureExecution() {
	}
}
