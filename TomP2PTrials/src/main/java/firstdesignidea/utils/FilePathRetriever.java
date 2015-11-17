package firstdesignidea.utils;

import java.io.File;
import java.util.List;

public enum FilePathRetriever {

	INSTANCE;

	public void getFiles(File f, List<String> pathVisitor) {
		File files[];

		if (f.isFile())
			pathVisitor.add(f.getAbsolutePath());
		else {
			files = f.listFiles();
			for (int i = 0; i < files.length; i++) {
				getFiles(files[i], pathVisitor);
			}
		}
	}

}