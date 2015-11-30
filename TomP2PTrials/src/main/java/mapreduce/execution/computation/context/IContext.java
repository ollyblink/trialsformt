package mapreduce.execution.computation.context;

import mapreduce.execution.jobtask.Task;

public interface IContext {

	public void write(Object keyOut, Object valueOut);
	
	public IContext task(Task task);

}
