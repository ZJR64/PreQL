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
     *
     * @param input
     */
    public WhereClause(String input) {
        this.root = parseTree(input.strip());
    }

    // function to get the root node
    public Node getRoot() {
        return this.root;
    }

    /**
     * recursively parses the where clause into a tree
     *
     * @param input
     * @return the root node
     */
    private Node parseTree(String input) {
        // if there's an or, that's the root
        if (input.toLowerCase().contains(" or ")) {
            Node orNode = new Node("or", NodeType.OPERATOR);
            String[] splitOr = input.split(" or ", 2);
            orNode.addLeftNode(parseTree(splitOr[0]));
            orNode.addRightNode(parseTree(splitOr[1]));
            return orNode;
        }
        // next, check for and
        else if (input.toLowerCase().contains(" and ")) {
            Node andNode = new Node("and", NodeType.OPERATOR);
            String[] splitAnd = input.split(" and ", 2);
            andNode.addLeftNode(parseTree(splitAnd[0]));
            andNode.addRightNode(parseTree(splitAnd[1]));
            return andNode;
        }
        // finally, expressions are left
        else {
            String[] splitExp = input.split(" ", 3);
            // middle element will be the operator and the root
            Node op = new Node(splitExp[1], NodeType.COMPARATOR);
            String leftString = splitExp[0];
            if(leftString.startsWith("\"") && leftString.endsWith("\"")){
                leftString = leftString.substring(1, leftString.length()-1);
            }
            op.addLeftNode(new Node(leftString, NodeType.VALUE));
            String rightString = splitExp[2];
            if(rightString.startsWith("\"") && rightString.endsWith("\"")){
                rightString = rightString.substring(1, rightString.length()-1);
            }
            op.addRightNode(new Node(rightString, NodeType.VALUE));
            return op;
        }
    }
}