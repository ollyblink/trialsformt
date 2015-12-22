package mapreduce.execution.computation.context;

import java.util.Set;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.task.Task;
import net.tomp2p.peers.Number160;

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

	@Override
	public Number160 resultHash() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IContext combiner(IMapReduceProcedure combiner) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMapReduceProcedure combiner() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Object> keys() {
		// TODO Auto-generated method stub
		return null;
	}

}
