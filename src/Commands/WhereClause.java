package src.Commands;

/**
 * Class for the wherecluase
 * parses the where clause and builds a tree based on input
 *
 * @author Kaitlyn DeCola kmd8594@rit.edu
 */
public class WhereClause {
    // root node of the tree
    private Node root;

    /**
     * constructor that calls the recursive parse function and sets the root node
     * @param input
     */
    public WhereClause(String input){
        this.root = parseTree(input.strip());
    }

    // function to get the root node
    public Node getRoot(){
        return this.root;
    }

    /**
     * recursively parses the where clause into a tree
     * @param input
     * @return the root node
     */
    private Node parseTree(String input){
        // if there's an or, that's the root
        if(input.toLowerCase().contains(" or ")){
            Node orNode = new Node("or", NodeType.OPERATOR);
            String[] splitOr = input.split(" or ", 2);
            orNode.addLeftNode(parseTree(splitOr[0]));
            orNode.addRightNode(parseTree(splitOr[1]));
            return orNode;
        }
        // next, check for and
        else if(input.toLowerCase().contains(" and ")){
            Node andNode = new Node("and", NodeType.OPERATOR);
            String[] splitAnd = input.split(" and ", 2);
            andNode.addLeftNode(parseTree(splitAnd[0]));
            andNode.addRightNode(parseTree(splitAnd[1]));
            return andNode;
        }
        // finally, expressions are left
        else{
            String[] splitExp = input.split(" ", 3);
            // middle element will be the operator and the root
            Node op = new Node(splitExp[1], NodeType.COMPARATOR);
            op.addLeftNode(new Node(splitExp[0], NodeType.VALUE));
            op.addRightNode(new Node(splitExp[2], NodeType.VALUE));
            return op;
        }
    }
}

/**
 * Node class
 */
class Node{
    NodeType type;
    String value;
    Node left;
    Node right;

    Node(String value, NodeType type){
        this.value = value;
        this.type = type;
    }

    public void addLeftNode(Node node){
        this.left = node;
    }

    public void addRightNode(Node node){
        this.right = node;
    }

    // returns the left node
    public Node getLeft(){
        return this.left;
    }

    // returns the right node
    public Node getRight(){
        return this.right;
    }
}

/**
 * the type of node
 */
enum NodeType{
    // and or or
    OPERATOR,
    // =, <, >,...
    COMPARATOR,
    // either a column or a value
    // will always be a leaf node
    VALUE
}
