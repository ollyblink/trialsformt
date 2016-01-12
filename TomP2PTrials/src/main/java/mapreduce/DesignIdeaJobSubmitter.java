package mapreduce;

import mapreduce.engine.executor.MRJobSubmissionManager;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.utils.FileSize;

public class DesignIdeaJobSubmitter {
	public static void main(String[] args)
	// throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, ClassNotFoundException, InstantiationException
	{
		String bootstrapIP = "192.168.43.65";
		int bootstrapPort = 4000;

		MRJobSubmissionManager submitter = MRJobSubmissionManager.create(DHTConnectionProvider.create(bootstrapIP, bootstrapPort));

		// String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/firstdesignidea/execution/datasplitting/testfile";
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/manager/testFiles";
		;
 
		Job job = Job.create(submitter.id(), null).fileInputFolderPath(fileInputFolderPath).maxFileSize(FileSize.THIRTY_TWO_KILO_BYTES)
				.addSucceedingProcedure(WordCountMapper.create(), null, 1, 1).addSucceedingProcedure(WordCountReducer.create(), null, 1, 1);

		submitter.submit(job);
		//
		//
		// DHTConnectionProvider dhtConnectionProvider = DHTConnectionProvider.newDHTConnectionProvider().bootstrapIP(bootstrapIP)
		// .bootstrapPort(bootstrapPort).broadcastDistributor(new MRBroadcastHandler());
		//
		// ITaskSplitter taskSplitter = MaxFileSizeTaskSplitter.newMaxFileSizeTaskSplitter();
		// MRJobSubmitter mRJS = MRJobSubmitter.newMapReduceJobSubmitter().dhtConnectionProvider(dhtConnectionProvider).taskSplitter(taskSplitter);
		//
		// IMapReduceProcedure<String, String, String, Integer> mapper = new IMapReduceProcedure<String, String, String, Integer>() {
		//
		// /**
		// *
		// */
		// private static final long serialVersionUID = 2783620472142008391L;
		//
		// @Override
		// public void process(String key, String value, IContext context) {
		// String[] words = value.split(" ");
		// for (String word : words) {
		// context.write(word, 1);
		// }
		// }
		//
		// };
		//
		// //
		// final Map<String, List<Integer>> ones = new TreeMap<String, List<Integer>>();
		// //
		//
		// IMapReduceProcedure<String, Iterable<Integer>, String, Integer> reducer = new IMapReduceProcedure<String, Iterable<Integer>, String,
		// Integer>() {
		//
		// @Override
		// public void process(String key, Iterable<Integer> values, IContext context) {
		// int sum = 0;
		// for (Integer i : values) {
		// sum += i;
		// }
		// context.write(key, sum);
		// }
		//
		// };
		//
		// IContext reducerContext = null;
		// // new IContext<String, Integer>() {
		// //
		// // @Override
		// // public void write(String keyOut, Integer valueOut) {
		// // System.out.println("<" + keyOut + "," + valueOut + ">");
		// // }
		// //
		// // };
		//
		// long maxFileSize = 1024 * 1024;
		// String inputPath = "/home/ozihler/Desktop/input_small";
		// String outputPath = "location/to/store/results";
		//
		// Job job = Job.newJob().procedures(mapper).procedures(reducer).inputPath(inputPath).outputPath(outputPath).maxFileSize(maxFileSize);
		//
		// System.out.println("Jobsubmission");
		// // mRJS.submit(job);
		//
		// // int i = 0;
		// // for (IMapReduceProcedure<?, ?, ?, ?> p : job.procedures()) {
		// Method process = mapper.getClass().getMethods()[0];
		//
		// Type c1 = ((ParameterizedType) mapper.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0];
		// Type c2 = ((ParameterizedType) mapper.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[1];
		// Type c3 = ((ParameterizedType) mapper.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[2];
		// Type c4 = ((ParameterizedType) mapper.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[3];
		//
		// // if (i == 0) {
		// System.out.println("In first");
		// Class<?> class1 = Class.forName(c1.getTypeName());
		// Class<?> class2 = Class.forName(c2.getTypeName());
		// final Class<?> class3 = Class.forName(c3.getTypeName());
		// Class<?> class4 = Class.forName(c4.getTypeName());
		//
		// IContext mapperContext = new IContext() {
		//
		// @Override
		// public void write(Object keyOut, Object valueOut) {
		// // Object cast = class3.cast(keyOut);
		//
		// }
		// };
		//
		// process.invoke(mapper, new Object[] { class4.cast("Hello"), class2.cast("this this is is this is a this"), mapperContext });
		// // i++;
		// // } else if (i == 1) {
		// // System.out.println("In second");
		// // for (Object word : ones.keySet()) {
		// // // System.out.println(word.getClass());
		// // // if (Class.forName(c1.getTypeName()).newInstance().getClass().isInstance(word) &&
		// // // Class.forName(c2.getTypeName()).isInstance(word)) {
		// // process.invoke(p, new Object[] { word, ones.get(word), reducerContext });
		// // // }
		// // }
		// // i++;
		// // }
		// // }
		//
		// // FutureJobCompletion completion = mRJS.awaitCompletion();
		// // completion.addListener(new BaseFutureListener<FutureJobCompletion>(){
		// //
		// // @Override
		// // public void operationComplete(FutureJobCompletion future) throws Exception {
		// // if(future.isSuccess()){
		// // Map<ITask, List<PeerAddress>> results = future.locations();
		// // for(ITask task: results.keySet()){
		// // List<PeerAddress> list = results.get(task);
		// // for(PeerAddress p: list){
		// // //Connect to p
		// // //Request data stored on p for ITask task
		// // //Transfer data from p to here
		// // //If all data is transferred: break;
		// // //else: either discard all and request all data from next PeerAddress
		// // //or only request difference from next PeerAddress
		// // //If not all data for a task could be received: mark job as failed and reschedule
		// // }
		// // }
		// // }else{
		// // System.err.println("Error occured");
		// // }
		// // }
		// //
		// // @Override
		// // public void exceptionCaught(Throwable t) throws Exception {
		// // t.printStackTrace();
		// // }
		// //
		// // });
	}
}
