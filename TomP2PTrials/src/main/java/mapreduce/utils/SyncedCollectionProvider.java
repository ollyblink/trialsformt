package mapreduce.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

public class SyncedCollectionProvider {

	public static <T> List<T> syncedList() {
		return Collections.synchronizedList(new ArrayList<>());
	}

	public static <K, V> Map<K, V> syncedHashMap() {
		return Collections.synchronizedMap(new HashMap<>());
	}

	public static <K, V> Map<K, V> syncedTreeMap() {
		return Collections.synchronizedMap(new TreeMap<>());
	}

	public static <K, V> ListMultimap<K, V> syncedListMultimap() { 
		ArrayListMultimap<K, V> tmp3 = ArrayListMultimap.create();
		return Multimaps.synchronizedListMultimap(tmp3);

	}
}
