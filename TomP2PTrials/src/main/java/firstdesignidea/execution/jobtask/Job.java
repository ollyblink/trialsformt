package firstdesignidea.execution.jobtask;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import firstdesignidea.execution.computation.IMapReduceProcedure;

public class Job implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1152022246679324578L;
	private long maxFileSize;
	private String id;
	private Queue<IMapReduceProcedure<?, ?, ?, ?>> procedures;
	private String inputPath;
	private String outputPath;

	private Job() {
		Random random = new Random();
		id = "job" + "_" + System.currentTimeMillis() + "_" + random.nextLong();
		this.procedures = new LinkedList<IMapReduceProcedure<?, ?, ?, ?>>();
	}

	public static Job newJob() {
		return new Job();
	}

	public Job procedures(IMapReduceProcedure<?, ?, ?, ?>... procedures) {
		Collections.addAll(this.procedures, procedures);
		return this;
	}

	public Queue<IMapReduceProcedure<?, ?, ?, ?>> procedures() {
		return this.procedures;
	}

	public Job inputPath(String inputPath) {
		this.inputPath = inputPath;
		return this;
	}

	public String inputPath() {
		return inputPath;
	}

	public Job outputPath(String outputPath) {
		this.outputPath = outputPath;
		return this;
	}

	public String outputPath() {
		return outputPath;
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

}
