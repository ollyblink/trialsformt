package firstdesignidea.execution.jobtask;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import firstdesignidea.execution.computation.IMapReduceProcedure;
import firstdesignidea.execution.exceptions.FileLocationNotFoundException;
import firstdesignidea.execution.exceptions.NotSetException;

public class Job {
	private String id;
	private List<IMapReduceProcedure<?, ?, ?, ?>> procedures;
	private String inputPath;
	private String outputPath;
	private JobInformation jobInformation;
	private JobStatus currentStatus;

	private Job() {
		Random random = new Random();
		id = "job" + "_" + System.currentTimeMillis() + "_" + random.nextLong();
		this.procedures = new ArrayList<IMapReduceProcedure<?, ?, ?, ?>>();
	}

	public static Job newJob() {
		return new Job();
	}

	public Job inputPath(String inputPath) {
		this.inputPath = inputPath;
		return this;
	}

	public Job outputPath(String outputPath) {
		this.outputPath = outputPath;
		return this;
	}

	public Job procedures(IMapReduceProcedure<?, ?, ?, ?>... procedures) {
		Collections.addAll(this.procedures, procedures);
		return this;
	}

	public List<IMapReduceProcedure<?, ?, ?, ?>> procedures() {
		return this.procedures;
	}

	public String inputPath() throws FileLocationNotFoundException, NotSetException {
		if (this.inputPath != null) {
			if (new File(this.inputPath).exists()) {
				return this.inputPath;
			} else {
				throw new FileLocationNotFoundException("Could not find the file location specified.");
			}
		} else {
			throw new NotSetException("You need to specify a file location for the input file(s).");
		}
	}

	public String outputPath() throws FileLocationNotFoundException, NotSetException {
		if (this.outputPath != null) {
			if (new File(this.outputPath).exists()) {
				return this.outputPath;
			} else {
				throw new FileLocationNotFoundException("Could not find the file location specified.");
			}
		} else {
			throw new NotSetException("You need to specify a file location for the output file(s).");
		}
	}

	public String id() {
		return this.id;
	}
}
