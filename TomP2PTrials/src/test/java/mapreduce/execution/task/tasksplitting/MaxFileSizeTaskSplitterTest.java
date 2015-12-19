package mapreduce.execution.task.tasksplitting;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.execution.computation.standardprocedures.NullMapReduceProcedure;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.utils.FileUtils;
import mapreduce.utils.MaxFileSizeFileSplitter;

public class MaxFileSizeTaskSplitterTest {

	private static final boolean PRINT_RESULTS = true;
	private static MaxFileSizeFileSplitter dataSplitter;
	private static Job job;
	private static String outputPath;
	private static String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/task/tasksplitting/testfile";
	private static List<String> originalFileAllLines;
	private static int maxFileSize;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		outputPath = inputPath + "/tmp";
		if (new File(outputPath).exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(outputPath));
		}
		maxFileSize = 1024 * 1024;
		System.out.println(inputPath);
		job = Job.create("ME").inputPath(inputPath).maxFileSize(maxFileSize).nextProcedure(NullMapReduceProcedure.newInstance());
		dataSplitter = MaxFileSizeFileSplitter.newInstance();

		originalFileAllLines = readFile(inputPath + "/trial.txt");
		// originalFileAllLines.addAll(readFile(inputPath + "/file1.txt"));
		dataSplitter.split(job);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		FileUtils.INSTANCE.deleteTmpFolder(new File(outputPath));

	}

	private static List<String> readFile(String filePath) {

		List<String> lines = new ArrayList<String>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));

			String line = null;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}

	@Test
	public void testNumberOfLines() {
		int sum = 0;
		for (Task task : job.tasks(job.currentProcedureIndex())) {
			for (Object key : dataSplitter.keysForEachTask().get(task)) {
				String filePath = (String) key;
				sum += readFile(filePath).size();
			}
		}
		assertEquals(originalFileAllLines.size(), sum);
		if (PRINT_RESULTS) {
			System.err.println(originalFileAllLines.size() + "==" + sum);
		}
	}

	@Test
	public void testFileSize() {
		long sum = 0;
		for (Task task : job.tasks(job.currentProcedureIndex())) {
			for (Object key : dataSplitter.keysForEachTask().get(task)) {
				String filePath = (String) key;
				sum += new File(filePath).length();
			}
		}
		long expected = new File(inputPath + "/trial.txt").length();

		assertEquals(expected, sum);
		if (PRINT_RESULTS) {
			System.err.println(expected + "==" + sum);
		}
	}

	@Test
	public void testMaxFileSize() {
		for (Task task : job.tasks(job.currentProcedureIndex())) {
			for (Object key : dataSplitter.keysForEachTask().get(task)) {
				String filePath = (String) key;
				if (PRINT_RESULTS) {
					System.err.println(new File(filePath).length() + "<=" + maxFileSize);
				}
				assertTrue(new File(filePath).length() <= maxFileSize);

			}
		}
	}

}
