package mapreduce.execution.computation.context;

import mapreduce.execution.task.Task;
import net.tomp2p.peers.Number160;

public interface IContext {

	public void write(Object keyOut, Object valueOut);

	public IContext task(Task task);

	public Number160 resultHash();

}
