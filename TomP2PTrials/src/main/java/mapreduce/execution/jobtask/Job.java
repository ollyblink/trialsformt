package mapreduce.execution.jobtask;

import java.io.Serializable;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.computation.IMapReduceProcedure;

public class Job implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1152022246679324578L;
	private long maxFileSize;
	private String id;
	private TreeMap<IMapReduceProcedure<?, ?, ?, ?>, BlockingQueue<Task>> procedures;
	private String inputPath;
	private int maxNumberOfFinishedPeers;

	private Job() {
		Random random = new Random();
		id = "job" + "_" + System.currentTimeMillis() + "_" + random.nextLong();
		this.procedures = new TreeMap<IMapReduceProcedure<?, ?, ?, ?>, BlockingQueue<Task>>();
	}

	public static Job newJob() {
		return new Job();
	}

	public Job inputPath(String inputPath) {
		this.inputPath = inputPath;
		return this;
	}

	public String inputPath() {
		return inputPath;
	}

	public Job maxFileSize(long maxFileSize) {
		this.maxFileSize = maxFileSize;
		return this;
	}

	public long maxFileSize() {
		return this.maxFileSize;
	}

	public String id() {
		return this.id;
	}

	/**
	 * Make sure to put "null" for all procedures that have no assigned tasks yet
	 * 
	 * @param procedure
	 * @param tasksForProcedure
	 * @return
	 */
	public Job nextProcedure(IMapReduceProcedure<?, ?, ?, ?> procedure, BlockingQueue<Task> tasksForProcedure) {
		IMapReduceProcedure<?, ?, ?, ?> nextProcedure = nextProcedure();
		int procedureNr = 1;
		if (nextProcedure != null) {
			procedureNr = nextProcedure.procedureNr() + 1;
		}
		procedure.procedureNr(procedureNr);
		this.procedures.put(procedure, tasksForProcedure);
		return this;
	}

	/**
	 * Simply looks for the next procedure that has tasks assigned and returns those. If no tasks are assigned yet, the first procedure is returend.
	 * Else, null is returned
	 * 
	 * @return
	 */
	public IMapReduceProcedure<?, ?, ?, ?> nextProcedure() {
		IMapReduceProcedure<?, ?, ?, ?> nextProcedure = null;
		int counter = 0;
		for (IMapReduceProcedure<?, ?, ?, ?> p : procedures.keySet()) {
			if (counter++ == 0) {
				nextProcedure = p;
			} else if (procedures.get(p) != null) {
				nextProcedure = p;
			}
		}
		return nextProcedure;

	}

	public IMapReduceProcedure<?, ?, ?, ?> procedure(Integer procedureNr) {
		for (IMapReduceProcedure<?, ?, ?, ?> p : procedures.keySet()) {
			if (p.procedureNr().equals(procedureNr)) {
				return p;
			}
		}
		return null;
	}

	public BlockingQueue<Task> tasksFor(IMapReduceProcedure<?, ?, ?, ?> procedure) {
		return procedures.get(procedure);
	}

	public int maxNrOfFinishedPeers() {
		return this.maxNumberOfFinishedPeers;
	}

	public Job maxNrOfFinishedPeers(int maxNumberOfFinishedPeers) {
		this.maxNumberOfFinishedPeers = maxNumberOfFinishedPeers;
		return this;
	}
}
