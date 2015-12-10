package mapreduce.execution.task.taskexecutor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.task.Task;
import mapreduce.utils.IAbortableExecution;

public class ParallelTaskExecutor implements ITaskExecutor {
	private static Logger logger = LoggerFactory.getLogger(ParallelTaskExecutor.class);

	private ThreadPoolExecutor server;
	private List<Future<?>> currentThreads = new ArrayList<Future<?>>();
	private boolean abortedTaskExecution;

	private int nThreads;

	private ParallelTaskExecutor() {
		this.nThreads = Runtime.getRuntime().availableProcessors();
	}

	public static ParallelTaskExecutor newInstance() {
		return new ParallelTaskExecutor();
	}

	@Override
	public boolean abortedTaskExecution() {
		return this.abortedTaskExecution;
	}

	@Override
	public void executeTask(final Task task, final IContext context, final Multimap<Object, Object> dataForTask) {
		this.abortedTaskExecution = false;
		context.task(task);
		this.server = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		for (final Object key : dataForTask.keySet()) {
			Runnable run = new Runnable() {

				@Override
				public void run() {
					try {
						Method process = task.procedure().getClass().getMethods()[0];
						process.invoke(task.procedure(), new Object[] { key, dataForTask.get(key), context });
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			Future<?> submit = server.submit(run);
			this.currentThreads.add(submit);
		}
		cleanUp();

	}

	@Override
	public void abortTaskExecution() {
		this.abortedTaskExecution = true;
		logger.info("Aborting task");
		if (!server.isTerminated() && server.getActiveCount() > 0) {
			for (Future<?> run : this.currentThreads) {
				run.cancel(true);
			}
			cleanUp();
		}
		logger.info("Task aborted");
	}

	private void cleanUp() {
		server.shutdown();
		while (!server.isTerminated()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
