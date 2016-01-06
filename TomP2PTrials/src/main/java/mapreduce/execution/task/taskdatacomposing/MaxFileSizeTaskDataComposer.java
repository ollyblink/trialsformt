package mapreduce.execution.task.taskdatacomposing;

import java.nio.charset.Charset;

import mapreduce.utils.FileSize;

public class MaxFileSizeTaskDataComposer implements ITaskDataComposer {
	private FileSize maxFileSize = FileSize.THIRTY_TWO_KILO_BYTES;
	private String fileEncoding = "UTF-8";
	private String splitValue = "\n";

	private long fileSizeCounter = 0;
	private String data = "";

	public static MaxFileSizeTaskDataComposer create() {
		return new MaxFileSizeTaskDataComposer();
	}

	@Override
	public String append(String value) {
		String newData = this.data + value + this.splitValue;
		long newFileSizeCounter = newData.getBytes(Charset.forName(this.fileEncoding)).length;
		if (newFileSizeCounter >= maxFileSize.value()) {
			String currentData = this.data;
			reset();
			this.data = value + this.splitValue;
			this.fileSizeCounter = data.getBytes(Charset.forName(this.fileEncoding)).length;
			return currentData;
		} else {
			this.data = newData;
			this.fileSizeCounter = newFileSizeCounter;
			return null;
		}
	}

	@Override
	public void reset() {
		this.data = "";
		this.fileSizeCounter = 0;
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

}
