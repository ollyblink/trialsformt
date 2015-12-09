package mapreduce.execution.computation;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.task.Task;

public final class ProcedureTaskTupel implements Comparable<ProcedureTaskTupel>, Serializable {

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

	private ProcedureTaskTupel(IMapReduceProcedure procedure, BlockingQueue<Task> tasks) {
		this.procedure = procedure;
		this.tasks = tasks;
		this.procedureNumber = currentProcedureNumber;
		currentProcedureNumber += DEFAULT_PROCEDURE_NUMBER_INCREMENT;
	}

	public static ProcedureTaskTupel newProcedureTaskTupel(IMapReduceProcedure procedure, BlockingQueue<Task> tasks) {
		return new ProcedureTaskTupel(procedure, tasks);
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

	@Override
	public int compareTo(ProcedureTaskTupel o) {
		return procedureNumber.compareTo(o.procedureNumber);
	}
 
	@Override
	public String toString() {
		return this.procedure+", "+this.procedureNumber+", "+this.tasks;
	}

 

}
