package veera.topten.records;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TreeMapSortByValue {
    // Method for sorting the TreeMap based on values
    public static <K, V extends Comparable<V>> Map<K, V>
            sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator =
                new Comparator<K>() {
                    public int compare(K k1, K k2) {
                        int compare =
                                map.get(k1).compareTo(map.get(k2));
                        if (compare == 0)
                            return 1;
                        else if (compare > 0)
                            return -1;
                        else
                            return 1;
                    }
                };

        Map<K, V> sortedByValues =
                new TreeMap<K, V>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }

    public static void main(String args[]) {

        TreeMap<String, String> treemap = new TreeMap<String, String>();

        // Put elements to the map
        treemap.put("Key1", "Jack");
        treemap.put("Key2", "Rick");
        treemap.put("Key3", "Kate");
        treemap.put("Key4", "Tom");
        treemap.put("Key5", "Steve");

        TreeMap<String, Integer> treemap2 = new TreeMap<String, Integer>();

        // Put elements to the map
        treemap2.put("Key1", 1);
        treemap2.put("Key2", 0);
        treemap2.put("Key3", 10);
        treemap2.put("Key4", 2);
        treemap2.put("Key5", 89);
        treemap2.put("Key6", 2);
        // Calling the method sortByvalues
        Map sortedMap = sortByValues(treemap2);

        System.out.println("MAP " + sortedMap);

        // Get a set of the entries on the sorted map
        Set set = sortedMap.entrySet();

        // Get an iterator
        Iterator i = set.iterator();

        // Display elements
        while (i.hasNext()) {
            Map.Entry me = (Map.Entry) i.next();
            System.out.print(me.getKey() + ": ");
            System.out.println(me.getValue());
        }
    }

}
