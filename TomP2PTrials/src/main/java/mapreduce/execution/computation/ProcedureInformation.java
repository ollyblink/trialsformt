package mapreduce.execution.computation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapreduce.execution.task.Task;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Tuple;

public final class ProcedureInformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1717123684693430690L;
	private final String jobId;
	private final IMapReduceProcedure procedure;
	private final int procedureIndex;

	private boolean isFinished;
	private List<Task> tasks;

	private ProcedureInformation(String jobId, IMapReduceProcedure procedure, int procedureIndex) {
		this.jobId = jobId;
		this.procedure = procedure;
		this.procedureIndex = procedureIndex;
		this.isFinished = false;
		this.tasks = Collections.synchronizedList(new ArrayList<>());
	}

	public static ProcedureInformation create(String jobId, IMapReduceProcedure procedure, int procedureIndex) {
		return new ProcedureInformation(jobId, procedure, procedureIndex);
	}

	public IMapReduceProcedure procedure() {
		return procedure;
	}

	public boolean isFinished() {
		return this.isFinished;
	}

	public int procedureIndex() {
		return this.procedureIndex;
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
		result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
		result = prime * result + ((procedure == null) ? 0 : procedure.getClass().getSimpleName().hashCode());
		result = prime * result + procedureIndex;
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
		if (jobId == null) {
			if (other.jobId != null)
				return false;
		} else if (!jobId.equals(other.jobId))
			return false;
		if (procedure == null) {
			if (other.procedure != null)
				return false;
		} else if (!procedure.getClass().getSimpleName().equals(other.procedure.getClass().getSimpleName()))
			return false;
		if (procedureIndex != other.procedureIndex)
			return false;
		return true;
	}

	public Tuple<String, Tuple<String, Integer>> jobProcedureDomain() {
		return Tuple.create(jobId, Tuple.create(procedure.getClass().getSimpleName(), procedureIndex));
	}

	public String jobProcedureDomainString() {
		return DomainProvider.INSTANCE.jobProcedureDomain(jobProcedureDomain());
	}

	@Override
	public String toString() {
		return "ProcedureInformation [jobId=" + jobId + ", procedure=" + procedure + ", procedureIndex=" + procedureIndex + ", isFinished="
				+ isFinished + ", tasks=" + tasks + "]";
	}

}
