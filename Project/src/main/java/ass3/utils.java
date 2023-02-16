package ass3;


public class utils {
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
}