package src.Commands;

public class Node{
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

    public NodeType getType(){
        return this.type;
    }

    public String getValue(){
        return this.value;
    }
}
