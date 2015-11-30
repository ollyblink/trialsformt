package mapreduce.execution.computation.context;

import mapreduce.execution.jobtask.Task;

public class NullContext implements IContext {

	@Override
	public void write(Object keyOut, Object valueOut) {

	}

	private NullContext() {

	}

	public static NullContext newNullContext() {
		return new NullContext();
	}

	@Override
	public NullContext task(Task task) {
		// TODO Auto-generated method stub
		return null;
	}

}
