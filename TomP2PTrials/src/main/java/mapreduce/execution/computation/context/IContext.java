package mapreduce.execution.computation.context;

import java.util.Set;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.task.Task;
import net.tomp2p.peers.Number160;

public interface IContext {

	public void write(Object keyOut, Object valueOut);

	public IContext task(Task task);
	
	public Task task();

	public Number160 resultHash();

	public IContext combiner(IMapReduceProcedure combiner);

	public IMapReduceProcedure combiner();
	
	public void combine();

	public Set<Object> keys();

	public void resetResultHash();

	public void updateResultHash(Object keyOut, Object valueOut);

	public void broadcastResultHash();

}
