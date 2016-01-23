package mapreduce.engine.componenttests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class JobCalculationComponentTest {
	private static Logger logger = LoggerFactory.getLogger(JobCalculationComponentTest.class);
	private JobCalculationExecutor calculationExecutor;
	private JobCalculationMessageConsumer calculationMessageConsumer;
	private JobCalculationBroadcastHandler executorBCHandler;
	private IDHTConnectionProvider dhtCon;

	@Before
	public void setUp() throws Exception {
		calculationExecutor = JobCalculationExecutor.create();

		calculationMessageConsumer = JobCalculationMessageConsumer.create().executor(calculationExecutor);
		executorBCHandler = JobCalculationBroadcastHandler.create()
				.messageConsumer(calculationMessageConsumer);
		// int bootstrapPort = 4001;
		dhtCon = TestUtils.getTestConnectionProvider(executorBCHandler);
		// DHTConnectionProvider
		// .create("192.168.43.65", bootstrapPort, bootstrapPort).broadcastHandler(executorBCHandler)
		// .storageFilePath("C:\\Users\\Oliver\\Desktop\\storage")
		;
		dhtCon.broadcastHandler(executorBCHandler);
		calculationExecutor.dhtConnectionProvider(dhtCon);
		calculationMessageConsumer.dhtConnectionProvider(dhtCon);
	}

	@After
	public void tearDown() throws Exception {
		dhtCon.shutdown();
	}

	private static class Tuple {

		public Tuple(Task task, Object value) {
			this.task = task;
			this.value = value;
		}

		Task task;
		Object value;
	}

	@Test
	public void testAllOnceOneInitialTaskOneWord() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is only 1 initial task to execute
		// The task has only 1 word to count
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// ===========================================================================================================================================================

		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES)
				.addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false,
						false)
				.timeToLive(Long.MAX_VALUE)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		List<Tuple> tasks = new ArrayList<>();
		tasks.add(new Tuple(Task.create("testfile1", "S1"), "hello"));

		executeTest(job, tasks);
	}

	@Test
	public void testAllOnceOneInitialTaskMultipleWords() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is only 1 initial task to execute
		// The task has 9 words to count
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// ===========================================================================================================================================================

		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES)
				.addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false,
						false)
				.timeToLive(Long.MAX_VALUE)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		List<Tuple> tasks = new ArrayList<>();
		tasks.add(new Tuple(Task.create("testfile1", "S1"), "the quick fox jumps over the lazy brown dog"));

		executeTest(job, tasks);
	}

	@Test
	public void testAllOnceOneInitialTaskMultipleSameInitialTasks() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is only 2 initial tasks to execute
		// The tasks have 9 words each (twice the same) to count
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// ===========================================================================================================================================================

		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES)
				.addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false,
						false)
				.timeToLive(Long.MAX_VALUE)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		List<Tuple> tasks = new ArrayList<>();
		tasks.add(new Tuple(Task.create("testfile1", "S1"), "the quick fox jumps over the lazy brown dog"));
		tasks.add(new Tuple(Task.create("testfile2", "S1"), "the quick fox jumps over the lazy brown dog"));

		executeTest(job, tasks);
	}

	@Test
	public void testAllOnceOneInitialTaskMultipleDifferentInitialTasks() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is only 3 initial tasks to execute
		// The tasks have 8 words each (twice the same) to count
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// ===========================================================================================================================================================

		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES)
				.addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false,
						false)
				.timeToLive(Long.MAX_VALUE)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		List<Tuple> tasks = new ArrayList<>();
		int counter = 0;
		tasks.add(new Tuple(Task.create("testfile_" + counter++, "S1"),
				"the quick fox jumps over the lazy brown dog"));
		tasks.add(
				new Tuple(Task.create("testfile_" + counter++, "S1"), "sphinx of black quartz judge my vow"));
		tasks.add(new Tuple(Task.create("testfile_" + counter++, "S1"),
				"the five boxing wizards jump quickly"));

		executeTest(job, tasks);
	}

	@Test
	public void testAllOnceExternalInputFile() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is an external file to be processed
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// ===========================================================================================================================================================
		String text = FileUtils.INSTANCE.readLines(System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/componenttests/testfile.txt");
		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES)
				.addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false,
						false)
				.timeToLive(Long.MAX_VALUE)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		List<Tuple> tasks = new ArrayList<>();
		int counter = 0;
		tasks.add(new Tuple(Task.create("testfile_" + counter++, "S1"), text));

		executeTest(job, tasks);
	}

	private void executeTest(Job job, List<Tuple> tasks) throws ClassNotFoundException, IOException {
		HashMap<String, Integer> res = getCounts(tasks);
		execute(job, tasks);

		FutureGet getKeys = dhtCon
				.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS,
						executorBCHandler.getJob(job.id()).currentProcedure().dataInputDomain().toString())
				.awaitUninterruptibly();
		if (getKeys.isSuccess()) {
			Set<Number640> keySet = getKeys.dataMap().keySet();
			List<String> resultKeys = new ArrayList<>();
			for (Number640 keyN : keySet) {
				String outKey = (String) getKeys.dataMap().get(keyN).object();
				resultKeys.add(outKey);
			}
			assertEquals(res.keySet().size(), resultKeys.size());
			for (String key : res.keySet()) {
				assertEquals(true, resultKeys.contains(key));
				checkGets(job, key, 1, res.get(key));
			}
		}
	}

	private HashMap<String, Integer> getCounts(List<Tuple> tasks) {
		HashMap<String, Integer> res = new HashMap<>();
		for (Tuple tuple : tasks) {
			String valueString = (String) tuple.value;
			StringTokenizer tokens = new StringTokenizer(valueString);
			while (tokens.hasMoreTokens()) {
				String word = tokens.nextToken();
				Integer count = res.get(word);
				if (count == null) {
					count = 0; 
				}
				res.put(word, ++count);
			}

		}
		return res;
	}

	private void checkGets(Job job, String key, int nrOfValues, int sum)
			throws ClassNotFoundException, IOException {
		FutureGet getValues = dhtCon
				.getAll(key,
						executorBCHandler.getJob(job.id()).currentProcedure().dataInputDomain().toString())
				.awaitUninterruptibly();
		if (getValues.isSuccess()) {
			Set<Number640> valueSet = getValues.dataMap().keySet();
			assertEquals(1, valueSet.size());
			List<Integer> resultValues = new ArrayList<>();
			for (Number640 valueN : valueSet) {
				Integer outValue = (Integer) ((Value) getValues.dataMap().get(valueN).object()).value();
				resultValues.add(outValue);
			}
			assertEquals(nrOfValues, resultValues.size());
			assertEquals(true, resultValues.contains(sum));
			logger.info("Results: " + key + " with values " + resultValues);
		}
	}

	private void execute(Job job, List<Tuple> tasks) {

		// executorBCHandler.dhtConnectionProvider(dhtCon);

		logger.info("Procedures before put: " + job.procedures());
		dhtCon.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
		Procedure procedure = job.currentProcedure();
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), job.submissionCount(), "S1",
				procedure.executable().getClass().getSimpleName(), procedure.procedureIndex());
		procedure.dataInputDomain(JobProcedureDomain
				.create(job.id(), job.submissionCount(), "S1", DomainProvider.INITIAL_PROCEDURE, -1)
				.expectedNrOfFiles(tasks.size())).addOutputDomain(outputJPD);

		List<IBCMessage> msgs = new ArrayList<>();
		for (Tuple tuple : tasks) {
			ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(tuple.task.key(), "S1",
					tuple.task.newStatusIndex(), outputJPD);
			IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD)
					.dhtConnectionProvider(dhtCon);

			context.write(tuple.task.key(), tuple.value);
			Futures.whenAllSuccess(context.futurePutData()).awaitUninterruptibly();
			outputETD.resultHash(context.resultHash());
			IBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(outputETD,
					procedure.dataInputDomain());
			msgs.add(msg);
		}
		logger.info("Procedures before broadcast: " + job.procedures());

		for (IBCMessage msg : msgs) {
			dhtCon.broadcastCompletion(msg);
		}

		while (!executorBCHandler.getJob(job.id()).isFinished()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
