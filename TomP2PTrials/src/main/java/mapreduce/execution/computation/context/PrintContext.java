package mapreduce.execution.computation.context;

import mapreduce.execution.jobtask.Task;

public class PrintContext implements IContext {

	private Task task;

	@Override
	public void write(Object keyOut, Object valueOut) {
		System.out.println("Stored <K,V> pair for task " + task.id() + ": <" + keyOut + ", " + valueOut + ">");
	}

	private PrintContext() {

	}

	public static PrintContext newPrintContext() {
		return new PrintContext();
	}

	@Override
	public PrintContext task(Task task) {
		this.task = task;
		return this;
	}

}
