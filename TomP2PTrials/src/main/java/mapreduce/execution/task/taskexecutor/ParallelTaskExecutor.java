package mapreduce.execution.task.taskexecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.computation.context.IContext;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.utils.SyncedCollectionProvider;

public class ParallelTaskExecutor implements ITaskExecutor {
	private static Logger logger = LoggerFactory.getLogger(ParallelTaskExecutor.class);

	private ThreadPoolExecutor server;
	private List<Future<?>> currentThreads = SyncedCollectionProvider.syncedArrayList();
	private boolean abortedTaskExecution;

	private int nThreads;

	private ParallelTaskExecutor() {
		this.nThreads = Runtime.getRuntime().availableProcessors();
		this.server = new ThreadPoolExecutor(nThreads, nThreads, Long.MAX_VALUE, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

	}

	public static ParallelTaskExecutor newInstance() {
		return new ParallelTaskExecutor();
	}

	@Override
	public boolean abortedTaskExecution() {
		return this.abortedTaskExecution;
	}

	@Override
	public void execute(final IExecutable procedure, final Object key, final List<Object> values, final IContext context) {
//		System.err.println(nThreads + "==" + server.getActiveCount());
//		while (nThreads == server.getActiveCount()) {
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//		this.abortedTaskExecution = false;
//
//		this.currentThreads.add(server.submit(new Runnable() {
//
//			@Override
////			public void run() {
//				context.task().isActive(true);
//				procedure.process(key, values, context);
//				context.broadcastResultHash();
//				context.task().isActive(false);
//			}
//		}));

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
		this.server = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

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
