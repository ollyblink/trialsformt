package mapreduce.execution.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.ProcedureTaskTuple;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.PeerAddress;

public class Job implements Serializable {

	// private static Logger logger = LoggerFactory.getLogger(Job.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1152022246679324578L;

	private String jobSubmitterID;
	private long maxFileSize;
	private String id;
	private List<ProcedureTaskTuple> procedures;
	private String inputPath;
	private int maxNrOfFinishedWorkers;
	private int currentProcedureIndex;

	private Job(String jobSubmitterID) {
		this.jobSubmitterID = jobSubmitterID;
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.procedures = Collections.synchronizedList(new ArrayList<ProcedureTaskTuple>());
		this.currentProcedureIndex = 0;
	}

	public static Job newInstance(String jobSubmitterID) {
		return new Job(jobSubmitterID);
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

	public String jobSubmitterID() {
		return this.jobSubmitterID;
	}

	/**
	 * 
	 * @param procedure
	 * @param tasks
	 *            for this procedure
	 * @return
	 */
	public Job nextProcedure(IMapReduceProcedure procedure, Task... tasks) {
		List<Task> tasksAsList = new ArrayList<Task>();
		if (tasks != null) {
			Collections.addAll(tasksAsList, tasks);
		}
		return nextProcedure(procedure, tasksAsList);
	}

	public Job nextProcedure(IMapReduceProcedure procedure, List<Task> tasks) {
		if (procedure == null) {
			return this;
		}

		ProcedureTaskTuple procedureTasks = null;
		for (ProcedureTaskTuple p : procedures) {
			if (p.procedure().equals(procedure)) {
				procedureTasks = p;
			}
		}

		if (procedureTasks == null) {
			procedureTasks = ProcedureTaskTuple.create(procedure, new LinkedBlockingQueue<Task>());
		}

		if (tasks != null) {
			procedureTasks.tasks().addAll(tasks);
		}

		synchronized (procedures) {
			this.procedures.add(procedureTasks);
		}

		return this;
	}

	public IMapReduceProcedure firstProcedure() {
		return procedureTaskTupel(0).procedure();
	}

	public BlockingQueue<Task> firstTasks() {
		return procedureTaskTupel(0).tasks();
	}

	public IMapReduceProcedure lastProcedure() {
		return procedureTaskTupel(procedures.size() - 1).procedure();
	}

	public BlockingQueue<Task> lastTasks() {
		return procedureTaskTupel(procedures.size() - 1).tasks();
	}

	public IMapReduceProcedure procedure(int index) {
		return procedureTaskTupel(index).procedure();
	}

	public BlockingQueue<Task> tasks(int index) {
		if (index < procedures.size()) {
			return procedureTaskTupel(index).tasks();
		} else {
			return null;
		}
	}

	private ProcedureTaskTuple procedureTaskTupel(int index) {
		return this.procedures.get(index);
	}

	public BlockingQueue<Task> tasksFor(IMapReduceProcedure procedure) {
		for (ProcedureTaskTuple tupel : procedures) {
			if (tupel.procedure().equals(procedure)) {
				return tupel.tasks();
			}
		}
		return null;
	}

	public int maxNrOfFinishedWorkers() {
		if (this.maxNrOfFinishedWorkers == 0) {
			this.maxNrOfFinishedWorkers = 1;
		}
		return maxNrOfFinishedWorkers;
	}

	public Job maxNrOfFinishedWorkersPerTask(int maxNrOfFinishedWorkers) {
		if (maxNrOfFinishedWorkers < 1) {
			return this;
		}
		this.maxNrOfFinishedWorkers = maxNrOfFinishedWorkers;

		for (ProcedureTaskTuple tupel : procedures) {
			BlockingQueue<Task> tasks = tupel.tasks();
			if (tasks != null) {
				for (Task task : tasks) {
					task.maxNrOfFinishedWorkers(maxNrOfFinishedWorkers);
				}
			}
		}
		return this;
	}

	public void updateTaskExecutionStatus(String taskId, TaskResult toUpdate) {

		BlockingQueue<Task> tasks = procedures.get(currentProcedureIndex()).tasks();
		if (tasks != null) {
			for (Task task : tasks) {
				if (task.id().equals(taskId)) {
					task.updateStati(toUpdate);
					break;
				}
			}
		}
	}

	public void synchronizeFinishedTasksStati(Collection<Task> receivedSyncTasks) {
		BlockingQueue<Task> tasks = procedures.get(currentProcedureIndex()).tasks();
		tasks.clear();
		tasks.addAll(receivedSyncTasks);
	}

	// public void updateTaskFinalDataLocation(Task receivedTask) {
	// BlockingQueue<Task> tasks = procedures.get(currentProcedureIndex()).tasks();
	// if (tasks != null) {
	// for (Task task : tasks) {
	// if (task.equals(receivedTask)) {
	// task.finalDataLocation(receivedTask.finalDataLocation());
	// }
	// }
	// }
	// }

	public int currentProcedureIndex() {
		return currentProcedureIndex;
	}

	public void incrementProcedureNumber() {
		if (this.currentProcedureIndex < this.procedures.size() - 1) {
			++this.currentProcedureIndex;
		}
	}

	@Override
	public String toString() {
		return "Job [jobSubmitterID=" + jobSubmitterID + ", id=" + id + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		// if (getClass() != obj.getClass())
		// return false;
		Job other = (Job) obj;
		if (id == null) {
			if (other.id() != null)
				return false;
		} else if (!id.equals(other.id()))
			return false;
		return true;
	}

	public Multimap<Task, Tuple<PeerAddress, Integer>> taskDataToRemove(int currentProcedureIndex) {
		Multimap<Task, Tuple<PeerAddress, Integer>> toRemove = ArrayListMultimap.create();
		BlockingQueue<Task> tasks = tasks(currentProcedureIndex);
		for (Task task : tasks) {
			toRemove.putAll(task, task.dataToRemove());
		}
		return toRemove;
	}

	public boolean isFinishedFor(IMapReduceProcedure procedure) {
		for (ProcedureTaskTuple tuple : procedures) {
			if (tuple.procedure().equals(procedure)) {
				return tuple.isFinished();
			}
		}
		return false;
	}

	public void isFinishedFor(IMapReduceProcedure procedure, boolean isFinished) {
		for (ProcedureTaskTuple tuple : procedures) {
			if (tuple.procedure().equals(procedure)) {
				tuple.isFinished(isFinished);
				break;
			}
		}
	}

}
