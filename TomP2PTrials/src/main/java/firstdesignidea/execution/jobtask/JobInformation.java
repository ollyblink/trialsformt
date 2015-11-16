package firstdesignidea.execution.jobtask;

import java.util.HashSet;
import java.util.Set;

import firstdesignidea.execution.utils.FileSize;

public final class JobInformation {
	private Set<FileSize> filesAndSizes;

	private JobInformation() {

	}

	public static JobInformation newJobInformation() {
		return new JobInformation();
	}

	public JobInformation addFile(FileSize fileAndSize) {
		if (filesAndSizes == null) {
			filesAndSizes = new HashSet<FileSize>();
		}
		filesAndSizes.add(fileAndSize);
		return this;
	}

	public Set<FileSize> filesAndSizes() {
		return this.filesAndSizes;
	}
}
