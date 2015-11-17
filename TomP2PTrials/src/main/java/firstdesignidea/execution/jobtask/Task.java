package firstdesignidea.execution.jobtask;

import java.io.File;

import firstdesignidea.execution.computation.IMapReduceProcedure;

public class Task { 
	private String id;
	private String jobId;
	private IMapReduceProcedure<?, ?, ?, ?> procedure;
	private File file;

	private Task() {
	}
	
	public static Task newTask(){
		return new Task();
	}

	public String id() {
		return id;
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

	public Task procedure(IMapReduceProcedure<?, ?, ?, ?> procedure) {
		this.procedure = procedure;
		return this;
	}

	public Task file(File file) {
		this.file = file;
		return this;
	}
}
