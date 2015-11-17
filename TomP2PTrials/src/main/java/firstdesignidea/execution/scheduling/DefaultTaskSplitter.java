package firstdesignidea.execution.scheduling;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.storage.DHTConnectionProvider;
import firstdesignidea.utils.FilePathRetriever;

public final class DefaultTaskSplitter implements ITaskSplitter {

	@Override
	public void splitAndPut(Job job, DHTConnectionProvider dhtConnectionProvider) {
		// Get all the file paths from the input location!
		List<String> pathVisitor = new ArrayList<String>();
		FilePathRetriever.INSTANCE.getFiles(new File(job.inputPath()), pathVisitor);

		new File(job.inputPath() + "/tmp/").mkdirs();

		Map<String, List<String>> oldAndNew = new HashMap<String, List<String>>();

		for (String filePath : pathVisitor) {
			File file = new File(filePath);

			String fileName = file.getName().substring(0, (file.getName().lastIndexOf(".")));
			String extension = file.getName().replace(fileName, "");

			if (file.length() > job.maxFileSize()) {
				long filePartitions = (file.length() / job.maxFileSize());

				for (int i = 0; i < filePartitions; ++i) {

					String newFileLocation = file.getParent() + "/tmp/" + fileName + "_" + (i + 1) + extension;
					List<String> newFilePaths = oldAndNew.get(filePath);
					if (newFilePaths == null) {
						newFilePaths = new ArrayList<String>();
						oldAndNew.put(filePath, newFilePaths);
					}
					newFilePaths.add(newFileLocation);
				}
			} else {
				List<String> newFilePaths = oldAndNew.get(filePath);
				if (newFilePaths == null) {
					newFilePaths = new ArrayList<String>();
					oldAndNew.put(filePath, newFilePaths);
				}
				newFilePaths.add(filePath);

			}
		}

		// System.out.println(oldAndNew.values().size());

		for (String fileLocation : oldAndNew.keySet()) {
			try {
				BufferedReader reader = new BufferedReader(new FileReader(new File(fileLocation)));

				List<String> newFileLocations = oldAndNew.get(fileLocation);
				String line = null;
				for (String newFileLocation : newFileLocations) {
					File newFile = new File(newFileLocation);
					BufferedWriter writer = new BufferedWriter(new FileWriter(newFile));
					System.out.println(newFileLocation + ": " + new File(newFileLocation).length());
					while (((line = reader.readLine()) != null) && new File(newFileLocation).length() <= job.maxFileSize()) {
						writer.write(line + "\n");
						writer.flush();
					}
					writer.close();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		// List<Task> tasks = new ArrayList<Task>();
		// for (Task task : tasks) {
		// dhtConnectionProvider.addTask(job.id(), task);
		// }
	}

	public static void main(String[] args) {
		ITaskSplitter splitter = new DefaultTaskSplitter();
		Job job = Job.newJob().maxFileSize(1024 * 1024).inputPath("/home/ozihler/Desktop/tb");
		splitter.splitAndPut(job, null);
	}
}
