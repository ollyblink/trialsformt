package firstdesignidea.execution.jobtask;
import java.io.File;
import java.util.Random;

import firstdesignidea.execution.computation.combiner.ICombiner;
import firstdesignidea.execution.computation.combiner.NullCombiner;
import firstdesignidea.execution.computation.mapper.IMapper;
import firstdesignidea.execution.computation.mapper.NullMapper;
import firstdesignidea.execution.computation.reducer.IReducer;
import firstdesignidea.execution.computation.reducer.NullReducer;
import firstdesignidea.execution.exceptions.FileLocationNotFoundException;
import firstdesignidea.execution.exceptions.NotSetException;

public class Job {
	private String id;
	private IReducer<?, ?, ?, ?> reducer;
	private IMapper<?, ?, ?, ?> mapper;
	private ICombiner<?, ?, ?, ?> combiner;
	private String inputPath;
	private String outputPath;
//	private PeerDHT submissionNode;
	private Job previousJob;
	private Job nextJob;
	private JobInformation jobInformation;
	private JobStatus currentStatus;

	private Job() {
		Random random = new Random(); 
		id = "job"+"_"+System.currentTimeMillis()+"_"+random.nextLong() ;
	}

	public static Job newJob( ) {
		return new Job();
	}

	public Job mapper(IMapper<?, ?, ?, ?> mapper) {
		this.mapper = mapper;
		return this;
	}

	public Job reducer(IReducer<?, ?, ?, ?> reducer) {
		this.reducer = reducer;
		return this;
	}

	public Job combiner(ICombiner<?, ?, ?, ?> combiner) {
		this.combiner = combiner;
		return this;
	}

	public Job inputPath(String inputPath) {
		this.inputPath = inputPath;
		return this;
	}

	public Job outputPath(String outputPath) {
		this.outputPath = outputPath;
		return this;
	}

	public IReducer<?, ?, ?, ?> reducer() {
		if (this.reducer != null) {
			return this.reducer;
		} else {
			this.reducer(new NullReducer());
			return reducer;
		}
	}

	public IMapper<?, ?, ?, ?> mapper() {
		if (this.mapper != null) {
			return this.mapper;
		} else {
			this.mapper(new NullMapper());
			return this.mapper;
		}
	}

	public ICombiner<?, ?, ?, ?> combiner() {
		if (this.combiner != null) {
			return this.combiner;
		} else {
			this.combiner(new NullCombiner());
			return this.combiner;
		}
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
	
	/**
	 * Cascade jobs by appending the next job to a job
	 * @param job
	 */
	public Job appendJob(Job job){
		this.nextJob(job);
		job.previousJob(this);
		return this;
	}

	private Job previousJob(Job job) {
		this.previousJob = job;
		return this;
	}

	private void nextJob(Job job) {
		this.nextJob = job;
	}
	
	public String id(){
		return this.id;
	}
}
