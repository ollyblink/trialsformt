package mapreduce.execution.computation.context;

import mapreduce.execution.task.Task;
import net.tomp2p.peers.Number160;

public class PrintContext implements IContext {

	private Task task;

	@Override
	public void write(Object keyOut, Object valueOut) {
		System.out.println("Stored <K,V> pair for task " + task.id() + ": <" + keyOut + ", " + valueOut + ">");
		
	}

	private PrintContext() {

	}

	public static PrintContext newInstance() {
		return new PrintContext();
	}

	@Override
	public PrintContext task(Task task) {
		this.task = task;
		return this;
	}

	@Override
	public Number160 resultHash() {
		// TODO Auto-generated method stub
		return null;
	}

}
