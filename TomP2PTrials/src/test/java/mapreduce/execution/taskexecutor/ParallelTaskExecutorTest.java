package mapreduce.execution.taskexecutor;

import static org.junit.Assert.assertEquals;

import java.util.StringTokenizer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import mapreduce.execution.computation.context.PseudoStoreContext;
import mapreduce.execution.computation.standardprocedures.WordCountMapper;
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

	@Test
	public void test() throws InterruptedException {
		String text1 = "this is a text a text is a this";
		String text2 = "if this is this and that is that , makes this that this ?";
		Task task = Task.create("1").procedure(WordCountMapper.newInstance());
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

	@Test
	public void testParallelism() throws InterruptedException {
		// List<Object> copyList = new ArrayList<>();
		ITaskExecutor executor = ParallelTaskExecutor.newInstance();
		final Task task = Task.create("1").procedure(WordCountMapper.newInstance());

		ArrayListMultimap<Object, Object> tmp = ArrayListMultimap.create();
		ListMultimap<Object, Object> dataForTask = Multimaps.synchronizedListMultimap(tmp);
		PseudoStoreContext context = PseudoStoreContext.newInstance();

		Thread t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				while (!executor.abortedTaskExecution()) {
					while (dataForTask.isEmpty()) {
						try {
							Thread.sleep(1500);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					executor.executeTask(task, context, dataForTask);
				}

			}

		});

		String[] texts = { "hello world this is world", "my world is your world i don't care about it", "go make your own world",
				"This is another one I am trying out", "all they can do is see me rolling around", "out of the blue it blue in the out" };
		Thread t2 = new Thread(new Runnable() {
			private int counter = 1;

			@Override
			public void run() {
				for (String text : texts) {
					synchronized (dataForTask) {
						dataForTask.put("text" + counter++, text);
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				executor.abortedTaskExecution();

			}

		});

		t1.start();
		t2.start();
		Thread.sleep(10000);
		Multimap<Object, Object> invertedIndex = ArrayListMultimap.create();
		for (Object value : texts) {
			String valueString = (String) value;
			StringTokenizer tokens = new StringTokenizer(valueString);
			while (tokens.hasMoreTokens()) {
				invertedIndex.put(tokens.nextToken(), 1);
			}
		}

		ListMultimap<Object, Object> storage = context.storage();
		for (Object word : invertedIndex.keySet()) {
			System.out.println(word + " " + invertedIndex.get(word) + " ," + storage.get(word));
			assertEquals(invertedIndex.get(word).size(), storage.get(word).size());
		}
	}
}
