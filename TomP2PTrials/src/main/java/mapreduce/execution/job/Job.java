package mapreduce.execution.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.utils.FileSize;
import mapreduce.utils.IDCreator;

public class Job implements Serializable {

	// private static Logger logger = LoggerFactory.getLogger(Job.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1152022246679324578L;

	private static final FileSize DEFAULT_FILE_SIZE = FileSize.THIRTY_TWO_KILO_BYTES;

	private String id;
	private String jobSubmitterID;

	private List<ProcedureInformation> procedures;
	private int currentProcedureIndex;

	private FileSize maxFileSize = DEFAULT_FILE_SIZE;
	private String fileInputFolderPath;
	private int maxNrOfFinishedWorkers;

	private Job(String jobSubmitterID) {
		this.jobSubmitterID = jobSubmitterID;
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.currentProcedureIndex = 0;
		this.procedures = Collections.synchronizedList(new ArrayList<>());
	}

	public static Job create(String jobSubmitterID) {
		return new Job(jobSubmitterID);
	}

	public String id() {
		return this.id;
	}

	public String jobSubmitterID() {
		return this.jobSubmitterID;
	}

	public ProcedureInformation procedure(int index) {
		try {
			return procedures.get(index);
		} catch (Exception e) {
			return null;
		}
	}

	public Job procedure(IMapReduceProcedure procedure) {
		this.procedures.add(ProcedureInformation.create(procedure));
		return this;
	}

	public int maxNrOfFinishedWorkers() {
		return maxNrOfFinishedWorkers;
	}

	public Job maxNrOfFinishedWorkers(int maxNrOfFinishedWorkers) {
		this.maxNrOfFinishedWorkers = maxNrOfFinishedWorkers;
		return this;
	}

	public int currentProcedureIndex() {
		return currentProcedureIndex;
	}

	public void incrementProcedureNumber() {
		++this.currentProcedureIndex;
	}

	public Job fileInputFolderPath(String fileInputFolderPath) {
		this.fileInputFolderPath = fileInputFolderPath;
		return this;
	}

	public String fileInputFolderPath() {
		return fileInputFolderPath;
	}

	public Job maxFileSize(FileSize maxFileSize) {
		this.maxFileSize = maxFileSize;
		return this;
	}

	public FileSize maxFileSize() {
		return this.maxFileSize;
	}

	@Override
	public String toString() {
		return "Job [jobSubmitterID=" + jobSubmitterID + ", id=" + id + ", procedures=" + procedures + ", maxNrOfFinishedWorkers="
				+ maxNrOfFinishedWorkers + ", currentProcedureIndex=" + currentProcedureIndex + ", fileInputFolderPath=" + fileInputFolderPath
				+ ", maxFileSize=" + maxFileSize + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		// if (getClass() != obj.getClass())
		// return false;
		Job other = (Job) obj;
		if (id == null) {
			if (other.id() != null)
				return false;
		} else if (!id.equals(other.id()))
			return false;
		return true;
	}

}
