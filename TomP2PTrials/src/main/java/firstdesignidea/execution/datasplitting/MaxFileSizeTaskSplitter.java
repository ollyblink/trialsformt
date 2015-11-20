package firstdesignidea.execution.datasplitting;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.computation.IMapReduceProcedure;
import firstdesignidea.execution.computation.standardprocedures.NullMapReduceProcedure;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.storage.DHTConnectionProvider;
import firstdesignidea.utils.FileUtils;

public final class MaxFileSizeTaskSplitter implements ITaskSplitter {
	private static Logger logger = LoggerFactory.getLogger(MaxFileSizeTaskSplitter.class);

	private static final String ENDCODING = "UTF-8";

	private String tempFolderName;

	private MaxFileSizeTaskSplitter() {

	}

	public static MaxFileSizeTaskSplitter newMaxFileSizeTaskSplitter() {
		MaxFileSizeTaskSplitter splitter = new MaxFileSizeTaskSplitter();
		return splitter;
	}

	// public ITaskSplitter shouldDeleteAfterEmission(boolean shouldDeleteAfterEmission) {
	// this.shouldDeleteAfterEmission = shouldDeleteAfterEmission;
	// return this;
	// }

	private void readAllFilesAndCreateNewFiles(final Job job, final Map<String, List<String>> oldAndNew) {
		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		// long start = System.currentTimeMillis();

		for (final String fileLocation : oldAndNew.keySet()) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					readFileAndCreateNewFiles(job, oldAndNew, fileLocation);
				}
			});
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// long end = System.currentTimeMillis();
		// long ms = end - start;
		// long s = ms / 1000;
		// System.out.println("Execution took " + ms + " milliseconds");
	}

	private void createFinalTaskSplits(Job job, Collection<List<String>> allNewFileLocations) {

		int taskCounter = 1;
		IMapReduceProcedure<?, ?, ?, ?> firstProcedure = job.procedures().poll();
		if (firstProcedure != null) {
			for (List<String> locations : allNewFileLocations) {
				for (String location : locations) {
					List<String> keys = new ArrayList<String>();
					keys.add(location);
					job.tasks(Task.newTask().id((taskCounter++) + "").jobId(job.id()).keys(keys).procedure(firstProcedure, 1));
				}
			}
		} else {
			logger.error("Could not put job due to no procedure specified.");
		}
	}

	private void readFileAndCreateNewFiles(Job job, Map<String, List<String>> oldAndNew, String fileLocation) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(fileLocation)));

			List<String> newFileLocations = oldAndNew.get(fileLocation);
			String line = null;
			long fileSizeCounter = 0;
			for (String newFileLocation : newFileLocations) {
				BufferedWriter writer = new BufferedWriter(new FileWriter(new File(newFileLocation)));
				if (line != null) {
					writer.write(line + "\n");
					fileSizeCounter += (line + "\n").getBytes(ENDCODING).length;  
				}
				while (((line = reader.readLine()) != null)) {
					long lineSize = fileSizeCounter + (line + "\n").getBytes(ENDCODING).length;
					if (lineSize >= job.maxFileSize()) {
						break;
					}
					fileSizeCounter = lineSize;
					writer.write(line + "\n");

				}
				fileSizeCounter = 0;
				writer.flush();
				writer.close();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Map<String, List<String>> createNewFilePaths(Job job, List<String> pathVisitor) {
		Map<String, List<String>> oldAndNew = new TreeMap<String, List<String>>();

		for (String filePath : pathVisitor) {
			File file = new File(filePath);

			String fileName = file.getName().substring(0, (file.getName().lastIndexOf(".")));
			String extension = file.getName().replace(fileName, "");

			// To know how many files to create
			long filePartitions = (file.length() / job.maxFileSize())+1;

			// Put all the new filepaths into the map
			for (int i = 0; i < filePartitions; ++i) {
				String newFileLocation = file.getParent() + "/" + tempFolderName() + "/" + fileName + "_" + (i + 1) + extension;
				List<String> newFilePaths = oldAndNew.get(filePath);
				if (newFilePaths == null) {
					newFilePaths = new ArrayList<String>();
					oldAndNew.put(filePath, newFilePaths);
				}
				newFilePaths.add(newFileLocation);
			}
		}
		return oldAndNew;
	}

	private List<String> getFilePaths(Job job) {
		List<String> pathVisitor = new ArrayList<String>();
		FileUtils.INSTANCE.getFiles(new File(job.inputPath()), pathVisitor);
		return pathVisitor;
	}

	private File createTmpFolder(Job job) {
		File folder = new File(job.inputPath() + "/tmp/");
		if (folder.exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(folder);
		}
		folder.mkdirs();
		return folder;
	}

	public static void main(String[] args) {
		MaxFileSizeTaskSplitter splitter = MaxFileSizeTaskSplitter.newMaxFileSizeTaskSplitter();
		Job job = Job.newJob().maxFileSize(1024 * 1024).procedures(new NullMapReduceProcedure()).inputPath("/home/ozihler/Desktop/input_small");
		splitter.split(job);
		System.out.println(job.tasks());
		File file = new File(job.inputPath() + "/trial.csv");
		long sum = 0;
		for (Task t : job.tasks()) {
			for (int i = 0; i < t.keys().size(); ++i) {
				String key = (String) (t.keys().get(i));
				File file2 = new File(key);
				sum += file2.length();
			}
		}

		// FileUtils.INSTANCE.deleteTmpFolder(new File("/home/ozihler/Desktop/input_small/tmp"));
	}

	@Override
	public void splitAndEmit(final Job job, DHTConnectionProvider dhtConnectionProvider) {

		// // Finally, create tasks and emit all to the DHT
		// addToDHT(dhtConnectionProvider, job, oldAndNew.values());
		//
		// // Delete the temp folder after emission?
		// deleteTmpFolder(shouldDeleteAfterEmission, folder);
	}

	@Override
	public void split(Job job) {
		// Make a tmp folder for the copied files
		createTmpFolder(job);

		// Get all the file paths from the input location!
		List<String> pathVisitor = getFilePaths(job);

		// Uses this to store for each original file path the new splitted file paths
		final Map<String, List<String>> oldAndNew = createNewFilePaths(job, pathVisitor);

		// Create new files
		readAllFilesAndCreateNewFiles(job, oldAndNew);

		// Create the final tasks
		createFinalTaskSplits(job, oldAndNew.values());
	} 

	public MaxFileSizeTaskSplitter tempFolderName(String tempFolderName) {
		this.tempFolderName = tempFolderName;
		return this;
	}

	public String tempFolderName() {
		if (tempFolderName == null) {
			return "tmp";
		} else {
			return this.tempFolderName;
		}
	}
}
