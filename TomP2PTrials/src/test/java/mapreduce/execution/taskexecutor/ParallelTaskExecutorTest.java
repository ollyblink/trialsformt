package mapreduce.execution.taskexecutor;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.computation.context.PseudoStoreContext;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.taskexecutor.ParallelTaskExecutor;

public class ParallelTaskExecutorTest {

	private static ParallelTaskExecutor taskExecutor;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		taskExecutor = ParallelTaskExecutor.newInstance();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() throws InterruptedException {
		String text1 = "this is a text a text is a this";
		String text2 = "if this is this and that is that , makes this that this ?";
		Task task = Task.newInstance("1").procedure(new WordCountMapper());
		Multimap<Object, Object> dataForTask = ArrayListMultimap.create();
		dataForTask.put("text1", text1);
		dataForTask.put("text2", text2);
		PseudoStoreContext storeContext = PseudoStoreContext.newInstance();
		taskExecutor.executeTask(task, storeContext, dataForTask); 
		ListMultimap<Object, Object> storage = storeContext.storage();

		assertEquals(10, storage.keySet().size());
		assertEquals(3, storage.get("a").size());
		assertEquals(1, storage.get("and").size());
		assertEquals(1, storage.get("if").size());
		assertEquals(6, storage.get("this").size());
		assertEquals(4, storage.get("is").size());
		assertEquals(2, storage.get("text").size());
		assertEquals(1, storage.get("makes").size());
		assertEquals(1, storage.get(",").size());
		assertEquals(1, storage.get("?").size());
		assertEquals(3, storage.get("that").size());

		for (Object o : storage.keySet()) {
			assertEquals("String", o.getClass().getSimpleName());
			assertEquals("Integer", storage.get(o).get(0).getClass().getSimpleName());
		}
	}

}
