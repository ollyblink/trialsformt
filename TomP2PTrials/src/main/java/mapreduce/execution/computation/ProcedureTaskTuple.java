package mapreduce.execution.computation;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.task.Task;

public final class ProcedureTaskTuple implements Comparable<ProcedureTaskTuple>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8921776247682566166L;
	public static final Integer DEFAULT_FIRST_PROCEDURE_NUMBER = 1;
	public static final Integer DEFAULT_PROCEDURE_NUMBER_INCREMENT = 1;
	private static Integer currentProcedureNumber = DEFAULT_FIRST_PROCEDURE_NUMBER;

	private IMapReduceProcedure procedure;
	private BlockingQueue<Task> tasks;
	private Integer procedureNumber;
	private boolean isFinished;

	private ProcedureTaskTuple(IMapReduceProcedure procedure, BlockingQueue<Task> tasks) {
		this.procedure = procedure;
		this.tasks = tasks;
		this.procedureNumber = currentProcedureNumber;
		currentProcedureNumber += DEFAULT_PROCEDURE_NUMBER_INCREMENT;
		this.isFinished = false;
	}

	public static ProcedureTaskTuple create(IMapReduceProcedure procedure, BlockingQueue<Task> tasks) {
		return new ProcedureTaskTuple(procedure, tasks);
	}

	public IMapReduceProcedure procedure() {
		return procedure;
	}

	public BlockingQueue<Task> tasks() {
		return tasks;
	}

	public int procedureNumber() {
		return this.procedureNumber;
	}

	public boolean isFinished() {
		return this.isFinished;
	}

	public ProcedureTaskTuple isFinished(boolean isFinished) {
		this.isFinished = isFinished;
		return this;
	}

	@Override
	public int compareTo(ProcedureTaskTuple o) {
		return procedureNumber.compareTo(o.procedureNumber);
	}

	@Override
	public String toString() {
		return this.procedure + ", " + this.procedureNumber + ", " + this.tasks;
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
		ProcedureTaskTuple other = (ProcedureTaskTuple) obj;
		if (procedure == null) {
			if (other.procedure != null)
				return false;
		} else if (!procedure.equals(other.procedure))
			return false;
		return true;
	}

}
