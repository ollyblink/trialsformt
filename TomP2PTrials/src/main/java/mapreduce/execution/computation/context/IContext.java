package mapreduce.execution.computation.context;

import java.util.List;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.task.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.peers.Number160;

public interface IContext {

	public void write(Object keyOut, Object valueOut);

	public Number160 resultHash();

	public IContext combiner(IMapReduceProcedure combiner);

	public IMapReduceProcedure combiner();

	public void combine();

	public IContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider);

	public List<FuturePut> futurePutData();

	public DHTStorageContext taskExecutor(Tuple<String, Integer> taskExecutor);

	public AbstractBaseContext subsequentProcedure(ProcedureInformation subsequentProcedure);

	public ProcedureInformation subsequentProcedure();

	public IContext task(Task task);

	public Task task();
}
