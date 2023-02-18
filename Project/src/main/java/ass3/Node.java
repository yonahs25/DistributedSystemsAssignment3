package ass3;
// java imports
import java.util.LinkedList;
import java.util.List;

/**
 * Represents a node in the tree representation of a syntactic Ngram.
 */
class Node {
    
    private String word; // the original word
    private String stemmedWord; // the stemmed word
    private String posTag; // part of speech tag
    private String depLabel; // Stanford dependency label
    private int father; // the index of the node's parent in the ngram
    private List<Node> children; // the node's children in the ngram

    /**
     * Constructs a new Node instance.
     *
     * @param args The arguments of the node as an array of strings.
     * @param stemmer A Stemmer instance to stem the node's word.
     */
    Node(String[] args, Stemmer stemmer) {
        // Stem the word using the Porter stemmer algorithm
        stemmer.add(args[0].toCharArray(), args[0].length());
        stemmer.stem();
        this.word = args[0];
        this.stemmedWord = stemmer.toString();
        this.posTag = args[1];
        this.depLabel = args[2];
        try {
            // Parse the father index as an integer
            this.father = Integer.parseInt(args[3]);
        } catch (Exception e) {
            // If parsing fails, leave father as 0 (root node)
        }
        // Create an empty list of children
        this.children = new LinkedList<>();
    }
    
    /**
     * Adds a child to the node.
     */
    void addChild(Node child) {
        children.add(child);
    }

    /**
     * Returns the original word of the node.
     */
    public String getWord() {
        return word;
    }

    /**
     * Returns the stemmed word of the node.
     */
    String getStemmedWord() {
        return stemmedWord;
    }

    /**
     * Returns the part of speech tag of the node.
     */
    String getPosTag() {
        return posTag;
    }

    /**
     * Returns the Stanford dependency label of the node.
     */
    String getDepLabel() {
        return depLabel;
    }

    /**
     * Returns the index of the node's parent in the ngram.
     */
    int getFather() {
        return father;
    }

    /**
     * Returns the dependency path component of the node.
     * For example, the dependency path component of the node "cat_NN" would be "NN".
     */
    String getDependencyPathComponent() {
        return posTag;
    }

    /**
     * Returns true if the node is a noun, false otherwise.
     */
    boolean isNoun() {
        return posTag.equals("NN") || posTag.equals("NNS") || posTag.equals("NNP") || posTag.equals("NNPS");
    }

    /**
     * Returns a list of the node's children in the ngram.
     */
    List<Node> getChildren() {
        return children;
    }
}
