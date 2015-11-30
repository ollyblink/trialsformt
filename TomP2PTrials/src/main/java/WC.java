import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class WC {
	public static void main(String[] args) throws Exception {
		ArrayList<String> lines = getLinesFromFile(System.getProperty("user.dir") + "/src/text.txt");

		TreeMap<String, Set<Integer>> invertedIndex = new TreeMap<String, Set<Integer>>();
 
		for (int i = 0; i < lines.size(); ++i) {
			String[] words = lines.get(i).replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split(" ");
			for (String word : words) {
				Set<Integer> lineNumbers = invertedIndex.get(word);
				if (lineNumbers == null) {
					lineNumbers = new TreeSet<Integer>();
					invertedIndex.put(word, lineNumbers);
				}
				lineNumbers.add(i+1);
			} 
		}
		for (String word : invertedIndex.keySet()) {
			if (invertedIndex.get(word).size() > 1) {
				System.out.println(word + " " + invertedIndex.get(word));
			}
		}
	}

	private static ArrayList<String> getLinesFromFile(String filePath) {
		ArrayList<String> lines = new ArrayList<String>();
		Path file = Paths.get(filePath);

		Charset charset = Charset.forName("UTF-8");
		try (BufferedReader reader = Files.newBufferedReader(file, charset)) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
		} catch (IOException x) {
			System.err.format("IOException: %s%n", x);
		}
		return lines;
	}
}
