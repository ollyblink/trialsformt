package firstdesignidea.execution.scheduling;

import java.util.List;

import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;

public interface ITaskSplitter {

	public List<Task> split(Job job);

}
