package mapreduce.execution.task.taskdatacomposing;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import mapreduce.utils.FileSize;

public class MaxFileSizeTaskDataComposer implements ITaskDataComposer {
	private FileSize maxFileSize = FileSize.THIRTY_TWO_KILO_BYTES;
	private String fileEncoding = "UTF-8";
	private String splitValue = "\n";

	private String data = "";

	public static MaxFileSizeTaskDataComposer create() {
		return new MaxFileSizeTaskDataComposer();
	}

	@Override
	public String append(String line) {
		String newData = this.data + line + this.splitValue;
		long newFileSizeCounter = newData.getBytes(Charset.forName(this.fileEncoding)).length;
		if (newFileSizeCounter >= maxFileSize.value()) {
			String currentData = this.data;
			this.data = "";
			this.data = line + this.splitValue;
			if (currentData.trim().length() == 0) {
				return null;
			} else {
				return currentData;
			}
		} else {
			this.data = newData;
			return null;
		}
	}

	@Override
	public void reset() {
		this.data = "";
	}

	@Override
	public String fileEncoding() {
		return this.fileEncoding;
	}

	public MaxFileSizeTaskDataComposer fileEncoding(String fileEncoding) {
		this.fileEncoding = fileEncoding;
		return this;
	}

	public MaxFileSizeTaskDataComposer maxFileSize(FileSize maxFileSize) {
		this.maxFileSize = maxFileSize;
		return this;
	}

	public MaxFileSizeTaskDataComposer splitValue(String splitValue) {
		this.splitValue = splitValue;
		return this;
	}

	@Override
	public String currentValues() {
		return this.data;
	}

	@Override
	public List<String> splitToSize(String line) {
		List<String> splits = new ArrayList<>();
		String split = "";
		for (int i = 0; i < line.length(); ++i) {
			split += line.charAt(i);
			if (split.length() >= maxFileSize.value()) {
				splits.add(split);
				split = "";
			}
		}
		return splits;
	}

}
