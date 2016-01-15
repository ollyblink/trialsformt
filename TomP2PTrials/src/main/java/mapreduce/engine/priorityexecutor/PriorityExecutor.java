package mapreduce.engine.priorityexecutor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import mapreduce.engine.broadcasting.BCMessageStatus;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.task.Task;

/**
 * Taken from http://stackoverflow.com/questions/16833951/testing-priorityblockingqueue-in-threadpoolexecutor
 * 
 * @author Oliver
 *
 */
public class PriorityExecutor extends ThreadPoolExecutor {

	public PriorityExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	}
	// Utitlity method to create thread pool easily

	public static PriorityExecutor newFixedThreadPool(int nThreads) {
		return new PriorityExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new PriorityBlockingQueue<Runnable>());
	}
 
	// Submit with New comparable task

	public Future<?> submit(Runnable runnable, PriorityLevel jobPriority, Long jobCreationTime, Integer procedureIndex, BCMessageStatus messageStatus,
			Long messageCreationTime) {
		return super.submit(
				new ComparableBCMessageTask<>(runnable, null, jobPriority, jobCreationTime, procedureIndex, messageStatus, messageCreationTime));
	}

	public Future<?> submit(Runnable runnable, Task task) {
		return super.submit(new ComparableTaskExecutionTask<>(runnable, null, task));
	}
	// execute with New comparable task

	// public void execute(Runnable command, int priority) {
	// super.execute(new Comparable<>(command, null, priority));
	// }

	@Override
	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
		return (RunnableFuture<T>) callable;
	}

	//
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		return (RunnableFuture<T>) runnable;
	}

	public static void main(String[] args) throws InterruptedException {
		PriorityExecutor executorService = (PriorityExecutor) PriorityExecutor.newFixedThreadPool(1);
		// Future<?> submit = executorService.submit(new Runnable() {
		// @Override
		// public void run() {
		// while(submit.isCancelled())
		// System.out.println(i );
		// try {
		// Thread.sleep(20);
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
		// System.err.println("Here");
		// }
		// }, BCMessageStatus.COMPLETED_TASK.ordinal());

		// executorService.submit(getRunnable(BCMessageStatus.COMPLETED_PROCEDURE + "_2"), BCMessageStatus.COMPLETED_PROCEDURE.ordinal());
		// executorService.submit(getRunnable(BCMessageStatus.COMPLETED_TASK + "_3"), BCMessageStatus.COMPLETED_TASK.ordinal());
		// executorService.submit(getRunnable(BCMessageStatus.COMPLETED_PROCEDURE + "_4"), BCMessageStatus.COMPLETED_PROCEDURE.ordinal());
		// executorService.submit(getRunnable(BCMessageStatus.COMPLETED_TASK + "_5"), BCMessageStatus.COMPLETED_TASK.ordinal());
		// executorService.submit(getRunnable(BCMessageStatus.COMPLETED_PROCEDURE + "_6"), BCMessageStatus.COMPLETED_PROCEDURE.ordinal());
		// Future<?> submit2 = executorService.submit(getRunnable(BCMessageStatus.COMPLETED_TASK + "_7"), BCMessageStatus.COMPLETED_TASK.ordinal());
		// executorService.submit(getRunnable(BCMessageStatus.COMPLETED_PROCEDURE + "_8"), BCMessageStatus.COMPLETED_PROCEDURE.ordinal());
		Thread.sleep(100);
		// System.err.println(submit.cancel(true));
		// submit2.cancel(true);
		executorService.shutdown();
		try {
			executorService.awaitTermination(30, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// public static Runnable getRunnable(final String id) {
	// return new Runnable() {
	// @Override
	// public void run() {
	// for (int i = 0; i < 1000; ++i) {
	// System.out.println(i + " " + id);
	// try {
	// Thread.sleep(20);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// System.err.println("Here");
	// }
	// };
	// }
}