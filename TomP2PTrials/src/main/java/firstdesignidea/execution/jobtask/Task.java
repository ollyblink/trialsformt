package firstdesignidea.execution.jobtask;

import java.io.File;

import firstdesignidea.execution.computation.IMapReduceProcedure;

public class Task {
	private IMapReduceProcedure<?, ?, ?, ?> procedure;
	private File file;

	public Task(IMapReduceProcedure<?, ?, ?, ?> procedure, File file) {
		this.procedure = procedure;
		this.file = file;
	}

}
