package mapreduce.storage;

import java.util.List;

import mapreduce.execution.broadcasthandler.MRBroadcastHandler;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.KeyValuePair;
import mapreduce.execution.jobtask.Task;

public interface IDHTConnectionProvider {
 
	public void connect( );

	public <KEY, VALUE> void addDataForTask(String taskId, final KEY key, final VALUE value);

	public <KEY, VALUE> List<KeyValuePair<KEY, VALUE>> getDataForTask(Task task);

	public void broadcastNewJob(Job job);

	public void broadcastTaskSchedule(Task task);

	public void broadcastFinishedTask(Task task);

	public boolean alreadyExecuted(Task task);

	public void shutdown();

	public void broadcastFinishedAllTasks(Job job);

	public void broadcastFinishedJob(Job job);

	public MRBroadcastHandler broadcastHandler();
	
	public IDHTConnectionProvider useDiskStorage(boolean useDiskStorage);
	public boolean useDiskStorage();
}
