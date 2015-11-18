package firstdesignidea.execution.jobtask;

import java.io.File;
import java.io.Serializable;

import firstdesignidea.execution.computation.IMapReduceProcedure;

public class Task implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5374181867289486399L;
	private String id;
	private String jobId;
	private IMapReduceProcedure<?, ?, ?, ?> procedure;
	private File file;
	private int procedureIndex;

	private Task() {
	}

	public static Task newTask() {
		return new Task();
	}

	public String id() {
		return procedureIndex+"_"+id;
	}

	public String jobId() {
		return jobId;
	}

	public IMapReduceProcedure<?, ?, ?, ?> procedure() {
		return this.procedure;
	}

	public File file() {
		return this.file;
	}

	public Task id(String id) {
		this.id = id;
		return this;
	}

	public Task jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	/**
	 * 
	 * @param procedure
	 * @param procedureIndex specifies which procedure in the queue it is, used for task id
	 * @return
	 */
	public Task procedure(IMapReduceProcedure<?, ?, ?, ?> procedure, int procedureIndex) {
		this.procedure = procedure;
		this.procedureIndex = procedureIndex;
		return this;
	}

	public Task file(File file) {
		this.file = file;
		return this;
	}
}
