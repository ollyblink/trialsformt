package mapreduce.execution.taskexecutor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.computation.context.PseudoStorageContext;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.computation.standardprocedures.WordCountReducer;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.taskexecutor.ITaskExecutor;
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

	private static String text = "this is a text a text is a this if this is this and that is that , makes this that this ?";

	@Test
	public void test() throws InterruptedException {
		Task task = Task.newInstance("text1", "0");
		ProcedureInformation info = ProcedureInformation.create(WordCountMapper.newInstance()).addTask(task);

		PseudoStorageContext storeContext = PseudoStorageContext.newInstance();

		List<Task> ts = info.tasks();
		List<Object> values = new ArrayList<Object>();
		values.add(text);
		synchronized (ts) {
			for (Task t : ts) {
				taskExecutor.execute(info.procedure(), t.id(), values, storeContext);
			}
		}
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

	@Test
	public void testParallelism() throws InterruptedException {
		// List<Object> copyList = new ArrayList<>();
		ITaskExecutor executor = ParallelTaskExecutor.newInstance();

		PseudoStorageContext context = PseudoStorageContext.newInstance();
		String key = "text1";
		List<Object> values = Collections.synchronizedList(new ArrayList<>());

		context.combiner(WordCountReducer.newInstance());

		Thread t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				while (!executor.abortedTaskExecution()) {
					executor.execute(WordCountMapper.newInstance(), key, values, context);
					executor.abortTaskExecution();
				}

			}

		});

		Thread t2 = new Thread(new Runnable() {
			private int counter = 1;

			@Override
			public void run() {

				values.add(text);

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				executor.abortedTaskExecution();

			}

		});

		t1.start();
		t2.start();
		Thread.sleep(1000);
		Multimap<Object, Object> invertedIndex = ArrayListMultimap.create();

		String valueString = (String) values.get(0);
		StringTokenizer tokens = new StringTokenizer(valueString);
		while (tokens.hasMoreTokens()) {
			Object o = tokens.nextToken();
			invertedIndex.put(o, invertedIndex.removeAll(o).size()+1);
		}

		ListMultimap<Object, Object> storage = context.storage();
		for (Object word : invertedIndex.keySet()) {
			System.out.println(word + " " + invertedIndex.get(word) + " ," + storage.get(word));
			assertEquals(invertedIndex.get(word).size(), storage.get(word).size());
		}
	}
}
