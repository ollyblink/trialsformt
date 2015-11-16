package firstdesignidea.execution.utils;

public final class FileSize {
	private final String fileName;
	private final long fileSize;
	
	public FileSize(String fileName, long fileSize) {
		this.fileName = fileName;
		this.fileSize = fileSize;
	}
	
	public long fileSize(){
		return this.fileSize;
	}
	
	public String fileName(){
		return this.fileName;
	}
}
