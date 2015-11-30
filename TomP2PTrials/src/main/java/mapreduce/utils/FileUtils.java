package mapreduce.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public enum FileUtils {

	INSTANCE;

	public void deleteTmpFolder(File folder) {
		String[] entries = folder.list();
		for (String s : entries) {
			File currentFile = new File(folder.getPath(), s);
			currentFile.delete();
		}
		folder.delete();
	}

	public void getFiles(File f, List<String> pathVisitor) {

		if (f.isFile())
			pathVisitor.add(f.getAbsolutePath());
		else {
			File files[] = f.listFiles();
			if (files != null) {
				for (int i = 0; i < files.length; i++) {
					getFiles(files[i], pathVisitor);
				}
			}
		}
	}

	public String readLines(String filePath) throws FileNotFoundException, IOException {
		BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
		String line = null;
		String lines = "";
		while ((line = reader.readLine()) != null) {
			lines += line + "\n";
		}
		reader.close();
		return lines;
	}
}