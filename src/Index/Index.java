package src.Index;

import src.StorageManager.BufferManager;
import src.StorageManager.Record;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

public class Index {

    private int root;
    private String pageName;
    private BufferManager bufferManager;
    private String keyType;
    private int size;

    /**
     * Constructor for a NEW B+ tree.
     *
     * @param bufferManager The index buffer manager.
     * @param tableName The table name the b+ tree is being made for
     * @param keyType The type of what the B+ tree is indexing.
     * @param keySize The size of the key in Bytes.
     */

    public Index(BufferManager bufferManager, String tableName, String keyType, int keySize) {
        this.bufferManager = bufferManager;
        root = 0;
        this.pageName = tableName + ".idx";
        this.keyType = keyType;

        //get N for the tree
        int pairSize = keySize + Integer.BYTES;
        this.size = (int) (Math.floor(bufferManager.getPageSize()/ pairSize) - 1);
    }

    /**
     * Constructor for an ALREADY EXISTING B+ tree.
     *
     * @param bufferManager The index buffer manager.
     * @param tableName The table name the b+ tree is being made for
     * @param keyType The type of what the B+ tree is indexing.
     * @param buffer The bytebuffer containing all the data of the B+ tree.
     */
    public Index(BufferManager bufferManager, String tableName, String keyType, ByteBuffer buffer) {
        this.bufferManager = bufferManager;
        this.root = buffer.getInt();
        this.pageName = tableName + ".idx";
        this.keyType = keyType;
        this.size = buffer.getInt();
    }

    /**
     * takes an index and adds it to
     *
     * @param primaryKeyValue
     * @param pageNum
     * @param index
     */
    public void addToIndex(Object primaryKeyValue, int pageNum, int index) {
        //get leaf node
        Node currentNode = getToLeafNode(primaryKeyValue);
        Map<Object, Integer> temp = currentNode.getPageNums();
        if (temp.size() > size - 2) {

        }
    }

    /**
     * Removes the node associated with the primary key from the B+ tree.
     * @param primaryKeyValue The primary key value of the node being removed.
     */
    public void removeFromIndex(Object primaryKeyValue) {
        //TODO remove from the tree
    }

    /**
     * Splits a node into two when a node is full.
     */
    public void splitNode() {
        //TODO split node and add value to parent
    }

    /**
     * Gets a leaf node with the passed in primaryKeyValue
     *
     * @param primaryKeyValue The primaryKey value of the node being searched for.
     * @return The node at that level
     */
    public Node getToLeafNode(Object primaryKeyValue) {
        //setup initial
        byte[] nodeBytes = bufferManager.getPage(pageName, root);
        Node currentNode = new Node(nodeBytes, keyType);

        //loop through internal
        while (currentNode.isInternal()) {
            for (Map.Entry<Object, Integer> entry : currentNode.getPageNums().entrySet()) {
                Object key = entry.getKey();
                if (lessThan(key, primaryKeyValue)) {
                    nodeBytes = bufferManager.getPage(pageName, entry.getValue());
                    currentNode = new Node(nodeBytes, keyType);
                    break;
                }
            }
            nodeBytes = bufferManager.getPage(pageName, currentNode.getFinalValue());
            currentNode = new Node(nodeBytes, keyType);
        }

        return currentNode;
    }


    /**
     * Finds the node with a primary key equal to the passed in primaryKeyValue.
     * @param primaryKeyValue A primary key that is being searched for.
     * @return Null if the node doesn't exist, an int array containing the node's record's index,
     * and the nodes record's page number.
     */
    public int[] find(Object primaryKeyValue) {
        //get leaf node
        Node currentNode = getToLeafNode(primaryKeyValue);

        //get exact value
        for (Map.Entry<Object, Integer> entry : currentNode.getPageNums().entrySet()) {
            Object key = entry.getKey();
            if (equals(key, primaryKeyValue)) {
                int[] results = new int[2];
                results[0] = currentNode.getPageNums().get(key);
                results[1] = currentNode.getIndexes().get(key);
                return results;
            }
        }

        return null;
    }

    /**
     * Finds the node that is LESS than the passed in primaryKeyValue.
     *
     * @param primaryKeyValue A primary key whose lesser is being searched for.
     * @return Null if the node doesn't exist, an int array containing the node's record's index,
     * and the nodes record's page number.
     */
    public int[] findLessThan(Object primaryKeyValue) {
        //get leaf node
        Node currentNode = getToLeafNode(primaryKeyValue);

        //get exact value
        for (Map.Entry<Object, Integer> entry : currentNode.getPageNums().entrySet()) {
            Object key = entry.getKey();
            if (equals(key, primaryKeyValue)) {
                return null;
            }
            if (lessThan(key, primaryKeyValue)) {
                int[] results = new int[2];
                results[0] = currentNode.getPageNums().get(key);
                results[1] = currentNode.getIndexes().get(key);
                return results;
            }
        }
        return null;
    }

    /**
     * updates any affected nodes when the B+ tree upon splitting.
     * @param records The records of ...
     * @param pageNumber The pagenumber of ...
     */
    public void updateIndex(ArrayList<Record> records, int pageNumber) {
        for (int i = 0; i < records.size(); i++ ){
            Object key = records.get(i).getPrimaryKey();
            removeFromIndex(key);
            addToIndex(key, pageNumber, i);
        }
    }

    /**
     * converts the node to a bytearray.
     * @return a byte buffer containing the node.
     */
    public byte[] toBytes(){
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getIndexByteSize()]);
        buffer.putInt(root);
        buffer.putInt(size);
        return buffer.array();
    }

    /**
     * Checks if the two nodes primaryKey's are equivalent to each other.
     *
     * @param primaryKey the primarykey in the currentNode
     * @param primaryKeySearch the primarykey being searched
     * @return true if the primarykeys are equivalent, false otherwise.
     */
    public boolean equals(Object primaryKey, Object primaryKeySearch) {
        try {
            //look to see what type of attribute it is
            if (keyType.contains("char")) {
                //string
                String primaryKey1 = (String) primaryKey;
                String primaryKey2 = (String) primaryKeySearch;

                //compare
                if (primaryKey1.equals(primaryKey2)) {
                    return true;
                }
            }
            else if (keyType.equalsIgnoreCase("integer")) {
                //integer
                Integer primaryKey1 = (Integer) primaryKey;
                Integer primaryKey2 = (Integer) primaryKeySearch;

                //compare
                if (primaryKey1.equals(primaryKey2)) {
                    return true;
                }
            }
            else if (keyType.equalsIgnoreCase("boolean")) {
                //boolean, technincally it could be a key
                boolean primaryKey1 = (boolean) primaryKey;
                boolean primaryKey2 = (boolean) primaryKeySearch;

                //compare
                if (primaryKey1 == primaryKey2) {
                    return true;
                }
            }
            else {
                //must be double
                double primaryKey1 = (double) primaryKey;
                double primaryKey2 = (double) primaryKeySearch;

                //compare
                if (primaryKey1 == primaryKey2) {
                    return true;
                }
            }
        }
        catch (Exception e) {
            System.err.println(e);
        }
        return false;
    }


    //TODO returns true if primaryKey is less than primaryKeySearch

    /**
     * Determines whether the primarykey is less than the other primarykey.
     * @param primaryKey the pk value of the node being checked to see if
     *                   it's less than the other node.
     * @param primaryKeySearch the pk value of the other node who is used
     *                         as a comparison point for the other pk.
     * @return true if primaryKey < primaryKeySearch, false otherwise.
     */
    public boolean lessThan(Object primaryKey, Object primaryKeySearch) {
        try {
            //look to see what type of attribute it is
            if (keyType.contains("char")) {
                //string
                String primaryKey1 = (String) primaryKey;
                String primaryKey2 = (String) primaryKeySearch;

                //compare
                if (primaryKey1.compareTo(primaryKey2) < 0) {
                    return true;
                }
            }
            else if (keyType.equalsIgnoreCase("integer")) {
                //integer
                int primaryKey1 = (int) primaryKey;
                int primaryKey2 = (int) primaryKeySearch;

                //compare
                if (primaryKey1 < primaryKey2) {
                    return true;
                }
            }
            else if (keyType.equalsIgnoreCase("boolean")) {
                //boolean, technincally it could be a key
                boolean primaryKey1 = (boolean) primaryKey;
                boolean primaryKey2 = (boolean) primaryKeySearch;

                //compare
                if (primaryKey1 != primaryKey2) {
                    return true;
                }
            }
            else {
                //must be double
                double primaryKey1 = (double) primaryKey;
                double primaryKey2 = (double) primaryKeySearch;

                //compare
                if (primaryKey1 < primaryKey2) {
                    return true;
                }
            }
        }
        catch (Exception e) {}
        return false;
    }

    /**
     * calculates and returns the size of an index.
     * @return the size of the index in bytes.
     */
    public int getIndexByteSize() {
        //count both root int and int representing size
        return Integer.BYTES*2;
    }
}
