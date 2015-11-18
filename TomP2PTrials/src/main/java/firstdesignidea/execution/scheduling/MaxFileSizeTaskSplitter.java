package firstdesignidea.execution.scheduling;

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

import firstdesignidea.execution.computation.IMapReduceProcedure;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.storage.DHTConnectionProvider;
import firstdesignidea.utils.FileUtils;

public final class MaxFileSizeTaskSplitter implements ITaskSplitter {

	private static final String ENDCODING = "UTF-8";
	private boolean shouldDeleteAfterEmission;

	private MaxFileSizeTaskSplitter() {

	}

	public static MaxFileSizeTaskSplitter newTaskSplitter() {
		MaxFileSizeTaskSplitter splitter = new MaxFileSizeTaskSplitter();
		return splitter;
	}

	public ITaskSplitter shouldDeleteAfterEmission(boolean shouldDeleteAfterEmission) {
		this.shouldDeleteAfterEmission = shouldDeleteAfterEmission;
		return this;
	}

	@Override
	public void splitAndEmit(final Job job, DHTConnectionProvider dhtConnectionProvider) {
		// Make a tmp folder for the copied files
		File folder = createTmpFolder(job);

		// Get all the file paths from the input location!
		List<String> pathVisitor = getFilePaths(job);

		// Uses this to store for each original file path the new splitted file paths
		final Map<String, List<String>> oldAndNew = createNewFilePaths(job, pathVisitor);

		// Create new files
		readAllFilesAndCreateNewFiles(job, oldAndNew);

		// Finally, create tasks and emit all to the DHT
		addToDHT(dhtConnectionProvider, job, oldAndNew.values());

		// Delete the temp folder after emission?
		deleteTmpFolder(shouldDeleteAfterEmission, folder);
	}

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

	private void deleteTmpFolder(boolean shouldDeleteAfterEmission, File folder) {
		if (shouldDeleteAfterEmission) {
			FileUtils.INSTANCE.deleteTmpFolder(folder);
		}
	}

	private void addToDHT(DHTConnectionProvider dhtConnectionProvider, Job job, Collection<List<String>> allNewFileLocations) {

		int taskCounter = 1;
		IMapReduceProcedure<?, ?, ?, ?> firstProcedure = job.procedures().poll();
		if (firstProcedure != null) { 
			for (List<String> locations : allNewFileLocations) {
				for (String location : locations) {
					Task task = Task.newTask().file(new File(location)).jobId(job.id()).id((taskCounter++) + "").procedure(firstProcedure, 1);
					if(dhtConnectionProvider != null){
						dhtConnectionProvider.addTask(task);
					}else{
						System.err.println("Could not put job due to no dhtConnectionProvider specified.");
					}
				}
			}
		} else {
			System.err.println("Could not put job due to no procedure specified.");
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
				while (((line = reader.readLine()) != null) && fileSizeCounter <= job.maxFileSize()) {
					fileSizeCounter += line.getBytes(ENDCODING).length;
					writer.write(line + "\n");
					writer.flush();
				}
				fileSizeCounter = 0;
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
			long filePartitions = (file.length() / job.maxFileSize()) + 1;

			// Put all the new filepaths into the map
			for (int i = 0; i < filePartitions; ++i) {
				String newFileLocation = file.getParent() + "/tmp/" + fileName + "_" + (i + 1) + extension;
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
		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newTaskSplitter().shouldDeleteAfterEmission(true);
		Job job = Job.newJob().maxFileSize(1024 * 1024).inputPath("/home/ozihler/Desktop/input");
		splitter.splitAndEmit(job, null);
	}

}
