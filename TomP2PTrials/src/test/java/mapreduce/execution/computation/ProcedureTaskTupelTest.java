package mapreduce.execution.computation;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class ProcedureTaskTupelTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testComparability()
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, InstantiationException {
		List<ProcedureTaskTuple> tupels = new ArrayList<ProcedureTaskTuple>();
		for (int i = 0; i < 10; ++i) {
			ProcedureTaskTuple tupel = ProcedureTaskTuple.create(Mockito.mock(IMapReduceProcedure.class),
					Mockito.mock(LinkedBlockingQueue.class));
			tupels.add(tupel);
		}
		Collections.sort(tupels);
		Field tupel1Pnr = ProcedureTaskTuple.class.getDeclaredField("procedureNumber");
		tupel1Pnr.setAccessible(true);
		for (int i = 0; i < tupels.size() - 1; ++i) {
//			System.err.println(((Integer) tupel1Pnr.get(tupels.get(i)))+" <= "+((Integer) tupel1Pnr.get(tupels.get(i + 1))));
			assertTrue(((Integer) tupel1Pnr.get(tupels.get(i))) <= ((Integer) tupel1Pnr.get(tupels.get(i + 1))));

		}

	}

}
