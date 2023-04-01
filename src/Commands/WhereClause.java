package src.Commands;

/**
 * Class for the wherecluase
 * parses the where clause and builds a tree based on input
 *
 * @author Kaitlyn DeCola kmd8594@rit.edu
 */
public class WhereClause {
    // root node of the tree
    Node root;

    // currently builds the tree of the where clause "where num = 5"
    public WhereClause(String input){
        this.root = new Node("=", NodeType.COMPARATOR);
        root.addLeft("num", NodeType.VALUE);
        root.addLeft("5", NodeType.VALUE);
    }
}

class Node{
    NodeType type;
    String value;
    Node left;
    Node right;

    Node(String value, NodeType type){
        this.value = value;
        this.type = type;
    }

    public void addLeft(String value, NodeType type){
        this.left = new Node(value, type);
    }

    public void addRight(String value, NodeType type){
        this.right = new Node(value, type);
    }

    public Node getLeft(){
        return this.left;
    }

    public Node getRight(){
        return this.right;
    }
}

enum NodeType{
    // and or or
    OPERATOR,
    // =, <, >,...
    COMPARATOR,
    // either a column or a value
    VALUE
}
