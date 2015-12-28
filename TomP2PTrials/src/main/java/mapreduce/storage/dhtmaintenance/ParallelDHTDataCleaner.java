package mapreduce.storage.dhtmaintenance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

import mapreduce.execution.task.Task;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class ParallelDHTDataCleaner implements IDHTDataCleaner {
	private static Logger logger = LoggerFactory.getLogger(ParallelDHTDataCleaner.class);

	private ThreadPoolExecutor server;
	private List<Future<?>> currentThreads = new ArrayList<>();
	private List<CleanRunnable> cleaners = new ArrayList<>();
	private boolean abortedTaskExecution;
	private int nThreads;

	private String bootstrapIP;

	private int bootstrapPort;

	private ParallelDHTDataCleaner(String bootstrapIP, int bootstrapPort) {
		this.bootstrapIP = bootstrapIP;
		this.bootstrapPort = bootstrapPort;
		this.nThreads = Runtime.getRuntime().availableProcessors();

	}

	public static ParallelDHTDataCleaner newInstance(String bootstrapIP, int bootstrapPort) {
		return new ParallelDHTDataCleaner(bootstrapIP, bootstrapPort);
	}

	@Override
	public void removeDataFromDHT(Multimap<Task, Tuple<PeerAddress, Integer>> dataToRemove) {
		this.abortedTaskExecution = false;
		this.server = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		for (final Task task : dataToRemove.keySet()) {
			System.err.println(task);
			for (Tuple<PeerAddress, Integer> location : dataToRemove.get(task)) {
//				CleanRunnable cleaner = CleanRunnable.newInstance(bootstrapIP, bootstrapPort).dataToRemove(task, location); 
//				Future<?> submit = server.submit(cleaner);
//				this.currentThreads.add(submit);
//				this.cleaners.add(cleaner);
			}
		}
		cleanUp();
	}

	@Override
	public boolean abortedTaskExecution() {
		return this.abortedTaskExecution;
	}

	@Override
	public void abortTaskExecution() {
		this.abortedTaskExecution = true;
		if (!server.isTerminated() && server.getActiveCount() > 0) {
			for (Future<?> run : this.currentThreads) {
				run.cancel(true);
			}
		}
		cleanUp();
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
		for (CleanRunnable c : cleaners) {
//			c.shutdown();
		}
	}

}
