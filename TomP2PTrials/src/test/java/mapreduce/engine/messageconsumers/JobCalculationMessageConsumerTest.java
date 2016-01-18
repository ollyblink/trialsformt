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

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testHandleCompletedTask() {
		testHandleReceivedTaskMessage(); //Checks handleReceivedMessage() for a COMPLETED_TASK message
//		testHandleCompletedTaskMessage(); //Checks the actual method invoked
	}
	
	private void testHandleReceivedTaskMessage() {
		testInputNullTask(); //If any input is null, nothing should happen
		
	}

	
	
	private void testInputNullTask() {
		// TODO Auto-generated method stub
		
	}

	//Procedure
	@Test
	public void testHandleCompletedProcedure() {
		testHandleReceivedProcedureMessage(); //Checks handleReceivedMessage() for a COMPLETED_TASK message
	 
	}

	private void testHandleReceivedProcedureMessage() {
		testInputNullProcedure(); //If any input is null, nothing should happen
		
	}

	private void testInputNullProcedure() {
		// TODO Auto-generated method stub
		
	}
}
