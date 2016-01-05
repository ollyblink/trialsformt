package mapreduce.execution.context;

import java.util.List;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.storage.IDHTConnectionProvider;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.peers.Number160;

public interface IContext {

	public void write(Object keyOut, Object valueOut);

	public List<FuturePut> futurePutData();

	public Number160 resultHash(); 

	public IContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider);

	public IContext outputExecutorTaskDomain(ExecutorTaskDomain outputExecutorTaskDomain);

	// public IContext combiner(IExecutable combiner);

	// public IExecutable combiner();
	//
	// public void combine();
}
