package firstdesignidea.storage;

import java.io.Serializable;
import java.util.List;

import firstdesignidea.execution.broadcasthandler.broadcastobserver.IBroadcastDistributor;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;

public interface IDHTConnectionProvider {
	public void connect();

	public void broadcastNewJob(Job job);

	public <KEY, VALUE extends Serializable> void addDataForTask(String taskId, final KEY key, final VALUE value);

	public <VALUE extends Serializable> List<VALUE> getDataForTask(Task task);

	public IBroadcastDistributor broadcastDistributor();
}
