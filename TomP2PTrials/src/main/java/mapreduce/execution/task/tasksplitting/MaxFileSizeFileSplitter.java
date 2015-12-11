package mapreduce.execution.task.tasksplitting;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mapreduce.execution.job.Job;
import mapreduce.utils.FileUtils;

public enum MaxFileSizeFileSplitter {
	INSTANCE;
	// private static Logger logger = LoggerFactory.getLogger(MaxFileSizeTaskSplitter.class);

	private static final String DEFAULT_CHARSET = "UTF-8";
	private static final String TEMP_FOLDER_NAME = "tmp";
	private String fileEncoding = DEFAULT_CHARSET;
	private String fileInputFolderPath;
	private long maxFileSize;

	/**
	 * @param inputFolderPath
	 *            path to the folder in the file system containing the files to split
	 * 
	 * @param maxFileSize
	 *            files cannot be larger than this size (but smaller)
	 * 
	 * @param charset
	 *            charset to use on the files. if non specified: uses UTF-8
	 * @return
	 */
	public String split() {
		// Make a tmp folder for the copied files
		FileUtils.INSTANCE.createTmpFolder(fileInputFolderPath);

		// Get all the file paths from the input location!
		List<String> pathVisitor = new ArrayList<String>();
		FileUtils.INSTANCE.getFiles(new File(fileInputFolderPath), pathVisitor);

		// Uses this to store for each original file path the new splitted file paths
		final Map<String, List<String>> oldAndNew = createNewFilePaths(maxFileSize, pathVisitor);

		// Create new files
		readAllFilesAndCreateNewFiles(maxFileSize, oldAndNew);
		return fileInputFolderPath + "/" + TEMP_FOLDER_NAME;
	}

	private void readAllFilesAndCreateNewFiles(long maxFileSize, final Map<String, List<String>> oldAndNew) {
		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		for (final String fileLocation : oldAndNew.keySet()) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					readFileAndCreateNewFiles(maxFileSize, oldAndNew, fileLocation);
				}
			});
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Map<String, List<String>> createNewFilePaths(long maxFileSize, List<String> pathVisitor) {
		Map<String, List<String>> oldAndNew = new TreeMap<String, List<String>>();

		for (String filePath : pathVisitor) {
			File file = new File(filePath);

			String fileName = file.getName().substring(0, (file.getName().lastIndexOf(".")));
			String extension = file.getName().replace(fileName, "");

			// To know how many files to create
			long filePartitions = (file.length() / maxFileSize) + 1;

			// Put all the new filepaths into the map
			for (int i = 0; i < filePartitions; ++i) {
				String newFileLocation = file.getParent() + "/" + TEMP_FOLDER_NAME + "/" + fileName + "_" + (i + 1) + extension;
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

	private void readFileAndCreateNewFiles(long maxFileSize, Map<String, List<String>> oldAndNew, String fileLocation) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(fileLocation)));

			List<String> newFileLocations = oldAndNew.get(fileLocation);
			String line = null;
			long fileSizeCounter = 0;
			for (String newFileLocation : newFileLocations) {
				BufferedWriter writer = new BufferedWriter(new FileWriter(new File(newFileLocation)));
				if (line != null) {
					writer.write(line + "\n");
					fileSizeCounter += (line + "\n").getBytes(this.fileEncoding).length;
				}
				while (((line = reader.readLine()) != null)) {
					long lineSize = fileSizeCounter + (line + "\n").getBytes(this.fileEncoding).length;
					if (lineSize >= maxFileSize) {
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

	public String fileEncoding() {
		return fileEncoding;
	}

	public MaxFileSizeFileSplitter fileEncoding(String fileEncoding) {
		this.fileEncoding = fileEncoding;
		return this;
	}

	public MaxFileSizeFileSplitter fileInputFolderPath(String fileInputFolderPath) {
		this.fileInputFolderPath = fileInputFolderPath;
		return this;
	}

	public String fileInputFolderPath() {
		return fileInputFolderPath;
	}

	public MaxFileSizeFileSplitter maxFileSize(long maxFileSize) {
		this.maxFileSize = maxFileSize;
		return this;
	}

	public long maxFileSize() {
		return this.maxFileSize;
	}
}
