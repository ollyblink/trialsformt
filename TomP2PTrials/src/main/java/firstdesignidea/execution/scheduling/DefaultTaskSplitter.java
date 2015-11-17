package firstdesignidea.execution.scheduling;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.utils.FilePathRetriever;

public final class DefaultTaskSplitter implements ITaskSplitter {

	@Override
	public List<Task> split(Job job) {
		// Get all the file paths from the input location!
		List<String> pathVisitor = new ArrayList<String>();
		FilePathRetriever.INSTANCE.getFiles(new File(job.inputPath()), pathVisitor);

		
		for (String filePath : pathVisitor) {
			File file = new File(filePath);
			long fileSize = file.length();
			System.out.println(fileSize);
		}
		
		return new ArrayList<Task>();

	}

}
