package ass3;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class utils {

    //for job1

    // regex to remove all non-alphabetic characters from a string.
    static final String REGEX = "[^a-zA-Z ]+";
    private static Stemmer stemmer = new Stemmer();
    
    // create a Node array from a string array.
    public static Node[] createNodes(String[] splitted) {
        Node[] nodes = new Node[splitted.length];

        for (int i = 0; i < splitted.length; i++) {
            // split the string into its components.
            String[] ngramEntryComponents = splitted[i].split("/");
            if (ngramEntryComponents.length != 4) {
                return null;
            }
            // remove all non-alphabetic characters from the string.
            String firstComponent = ngramEntryComponents[0].replaceAll(REGEX, "");
            String secondComponent = ngramEntryComponents[1].replaceAll(REGEX, "");
            // if the string is empty, return null.
            if (firstComponent.equals("") || secondComponent.equals("")) {
                return null;
            }
            // create a new Node with the components from the string.
            nodes[i] = new Node(new String[]{firstComponent, secondComponent, ngramEntryComponents[2], ngramEntryComponents[3]}, stemmer);
        }
        return nodes;
    }

    // construct a tree from a Node array.
    public static Node constructParsedTree(Node[] nodes) {
        int rootIndex = 0;
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].getFather() > 0) {
                int fatherIndex = nodes[i].getFather() - 1;
                if (fatherIndex >= 0 && fatherIndex < nodes.length) {
                    nodes[fatherIndex].addChild(nodes[i]);
                }
            } else {
                rootIndex = i;
            }
        }
        return nodes[rootIndex];
    }

    public static int findIndexInPathsList(String path, File pathsListCopy) throws FileNotFoundException {
        Scanner scanner = new Scanner(pathsListCopy);
        int index = 0;
        while (scanner.hasNextLine()) {
            if (path.equals(scanner.nextLine())) {
                scanner.close();
                return index;
            }
            index++;
        }
        scanner.close();
        return -1;
    }

    //For job2

    /**
     * Adds the given value to the given features map at the given index.
     * If the index is already in the map, the value is added to the existing value at that index.
     * If the index is not in the map, the value is added as a new entry in the map.
     */
    public static void updateFeaturesMap(HashMap<Long, Long> features, long index, long val) {
        if (features.containsKey(index)) {
            features.put(index, features.get(index) + val);
        } else {
            features.put(index, val);
        }
    }
    
    /**
     * Constructs a vector string from the given features map, using the given key as the example ID.
     * The vector string starts with a "1" label, followed by a series of "index:value" pairs for each index
     * that has a non-zero value in the features map. The final part of the vector string is a comment
     * containing the key.
     */
    public static StringBuilder createVectorFromFeatures(HashMap<Long, Long> features, long numOfFeatures2, String key, HashMap<String, Boolean> testSet2) {
        StringBuilder vector = new StringBuilder();
        vector.append("1"); // label
        for (int i = 0; i < numOfFeatures2; i++) {
            addFeatureToVector(vector, features, i);
        }
        vector.append(" #" + key);
        String[] parts = key.split(",");
        if (testSet2.containsKey(parts[0]) || testSet2.containsKey(parts[1])) {
            vector.insert(0, "test\t");
        } else {
            vector.insert(0, "train\t");
        }
        return vector;
    }
    
    public static void addFeatureToVector(StringBuilder vector, HashMap<Long, Long> features, int i) {
        if (features.containsKey((long) i)) {
            vector.append(" " + (i + 1) + ":" + features.get((long) i));
        }
    }
    
}