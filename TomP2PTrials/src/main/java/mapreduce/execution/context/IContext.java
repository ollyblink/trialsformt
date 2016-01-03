package mapreduce.execution.context;

import java.util.List;

import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task2;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.peers.Number160;

public interface IContext {

	public void write(Object keyOut, Object valueOut);

	public Number160 resultHash();

	public IContext combiner(IExecutable combiner);

	public IExecutable combiner();

	public void combine();

	public IContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider);

	public List<FuturePut> futurePutData();

	public IContext taskExecutor(Tuple<String, Integer> taskExecutor);

	public IContext subsequentProcedure(Procedure subsequentProcedure);

	public Procedure subsequentProcedure();

	public IContext task(Task2 task);

	public Task2 task();
}
