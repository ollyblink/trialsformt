package mapreduce.execution.computation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.Tasks;

public class ProcedureInformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1717123684693430690L;
	private IMapReduceProcedure procedure;
	private boolean isFinished;
	private List<Task> tasks;

	private ProcedureInformation(IMapReduceProcedure procedure) {
		this.procedure = procedure;
		this.isFinished = false;
		this.tasks = Collections.synchronizedList(new ArrayList<>());
		// this.nrOfProcedureDomains = 0;
	}

	public static ProcedureInformation create(IMapReduceProcedure procedure) {
		return new ProcedureInformation(procedure);
	}

	public IMapReduceProcedure procedure() {
		return procedure;
	}

	public boolean isFinished() {
		return this.isFinished;
	}

	public ProcedureInformation isFinished(boolean isFinished) {
		this.isFinished = isFinished;
		return this;
	}

	public List<Task> tasks() {
		return this.tasks;
	}

	public ProcedureInformation addTask(Task task) {
		synchronized (this.tasks) {
			if (!this.tasks.contains(task)) {
				this.tasks.add(task);
			}
		}
		return this;
	}

	public ProcedureInformation tasks(List<Task> tasks) {
		this.tasks.clear();
		this.tasks.addAll(tasks);
		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((procedure == null) ? 0 : procedure.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProcedureInformation other = (ProcedureInformation) obj;
		if (procedure == null) {
			if (other.procedure != null)
				return false;
		} else if (!procedure.equals(other.procedure))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ProcedureInformation [procedure=" + procedure + ", isFinished=" + isFinished + ", tasks=" + tasks + "]";
	}

}
