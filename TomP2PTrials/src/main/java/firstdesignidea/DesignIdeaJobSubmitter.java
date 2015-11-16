package firstdesignidea;

import java.util.List;
import java.util.Map;

import firstdesignidea.client.MRJobSubmitter;
import firstdesignidea.execution.computation.combiner.ICombiner;
import firstdesignidea.execution.computation.combiner.NullCombiner;
import firstdesignidea.execution.computation.context.IContext;
import firstdesignidea.execution.computation.mapper.IMapper;
import firstdesignidea.execution.computation.reducer.IReducer;
import firstdesignidea.execution.jobtask.FutureJobCompletion;
import firstdesignidea.execution.jobtask.ITask;
import firstdesignidea.execution.jobtask.Job;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.PeerAddress;

public class DesignIdeaJobSubmitter {
	public static void main(String[] args) {

		String ip = "192.235.25.1";
		int port = 5000;

		MRJobSubmitter mRJS = MRJobSubmitter.newMapReduceJobSubmitter().ip(ip).port(port);

		IMapper<Object, String, String, Integer> mapper = new IMapper<Object, String, String, Integer>() {

			@Override
			public void map(Object key, String value, IContext<String, Integer> context) {
				String[] values = value.split(" ");
				for (String word : values) {
					context.write(word, 1);
				}
			}
		};
		
		IReducer<String, Integer, String, Integer> reducer = new IReducer<String, Integer, String, Integer>() {

			@Override
			public void reduce(String key, Iterable<Integer> values, IContext<String, Integer> context) {
				Integer sum = 0;
				for (Integer cnt : values) {
					sum += cnt;
				}
				context.write(key, sum);
			}
		};
		
		ICombiner<String, Integer, String, Integer> combiner = new NullCombiner();
		String inputPath = "location/to/data";
		String outputPath = "location/to/store/results";

		Job job = Job.newJob().mapper(mapper).reducer(reducer).combiner(combiner).inputPath(inputPath).outputPath(outputPath);
		Job job2 = Job.newJob().mapper(mapper).reducer(reducer);
		job.appendJob(job2);

		mRJS.submit(job);
		FutureJobCompletion completion = mRJS.awaitCompletion(); 
		completion.addListener(new BaseFutureListener<FutureJobCompletion>(){

			@Override
			public void operationComplete(FutureJobCompletion future) throws Exception {
				if(future.isSuccess()){
					Map<ITask, List<PeerAddress>> results = future.locations();
					for(ITask task: results.keySet()){
						List<PeerAddress> list = results.get(task);
						for(PeerAddress p: list){
							//Connect to p
							//Request data stored on p for ITask task
							//Transfer data from p to here
							//If all data is transferred: break;
							//else: either discard all and request all data from next PeerAddress
							//or only request difference from next PeerAddress
							//If not all data for a task could be received: mark job as failed and reschedule
						}
					}
				}else{
					System.err.println("Error occured");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				t.printStackTrace();
			}
			
		});
	}
}
