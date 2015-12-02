package mapreduce.execution.executor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Multimap;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.jobtask.Task;

public class ParallelTaskExecutor implements ITaskExecutor {

	private ThreadPoolExecutor server;
	private List<Future<?>> currentThreads = new ArrayList<Future<?>>();
	private boolean abortedTaskExecution;

	private ParallelTaskExecutor() {
	}

	public static ParallelTaskExecutor newInstance() {
		return new ParallelTaskExecutor();
	}

	@Override
	public void abortedTaskExecution(boolean abortedTaskExecution) {
		this.abortedTaskExecution = abortedTaskExecution;
	}

	@Override
	public boolean abortedTaskExecution() {
		return this.abortedTaskExecution;
	}

	@Override
	public void executeTask(final Task task, final IContext context, final Multimap<Object, Object> dataForTask) {
		context.task(task);
		int nThreads = Runtime.getRuntime().availableProcessors();
		this.server = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		for (final Object key : dataForTask.keySet()) {
			Runnable run = new Runnable() {

				@Override
				public void run() {
					callProcedure(key, dataForTask.get(key), task.procedure(), context);
				}
			};
			Future<?> submit = server.submit(run);
			this.currentThreads.add(submit);
		}
		cleanUp();

	}

	private void callProcedure(final Object key, final Collection<Object> values, final IMapReduceProcedure procedure, final IContext context) {

		try {
			Method process = procedure.getClass().getMethods()[0];
			process.invoke(procedure, new Object[] { key, values, context });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void abortTaskExecution() {
		this.abortedTaskExecution = true;
		System.err.println("Aborting task");
		if (!server.isTerminated() && server.getActiveCount() > 0) {
			for (Future<?> run : this.currentThreads) {
				run.cancel(true);
			}
			cleanUp();
		}
		System.err.println("Task aborted");
	}

	private void cleanUp() {
		server.shutdown();
		while (!server.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	
}
