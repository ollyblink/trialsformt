package mapreduce.execution.datasplitting;

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
import mapreduce.execution.datasplitting.MaxFileSizeTaskSplitter;
import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.Task;
import mapreduce.utils.FileUtils;

public class MaxFileSizeTaskSplitterTest {

	private static final boolean PRINT_RESULTS = true;
	private static MaxFileSizeTaskSplitter dataSplitter;
	private static String outputFolder;
	private static Job job;
	private static String outputPath;
	private static String inputPath;
	private static List<String> originalFileAllLines;
	private static int maxFileSize;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/datasplitting/testfile";
		outputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/datasplitting/testfile/tmp";

		System.out.println(new File(inputPath).exists());
		maxFileSize = 1024 * 1024;
		System.out.println(inputPath);
		job = Job.newJob("ME").inputPath(inputPath).maxFileSize(maxFileSize).nextProcedure(new NullMapReduceProcedure());
		dataSplitter = MaxFileSizeTaskSplitter.newMaxFileSizeTaskSplitter();

		originalFileAllLines = readFile(inputPath + "/trial.txt");
		// originalFileAllLines.addAll(readFile(inputPath + "/file1.txt"));
		dataSplitter.split(job);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// if (new File(outputFolder).exists()) {
		FileUtils.INSTANCE.deleteTmpFolder(new File(outputPath));
		// }

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
			for (Object key : task.keys()) {
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
			for (Object key : task.keys()) {
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
			for (Object key : task.keys()) {
				String filePath = (String) key;
				if (PRINT_RESULTS) {
					System.err.println(new File(filePath).length() + "<=" + maxFileSize);
				}
				assertTrue(new File(filePath).length() <= maxFileSize);

			}
		}
	}

}
