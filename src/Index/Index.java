package src.Index;

import src.StorageManager.BufferManager;
import src.StorageManager.Record;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class Index {

    private int root;
    private String pageName;
    private BufferManager bufferManager;
    private String keyType;
    private int size;
    private ArrayList<Integer> openPages;

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

        //create open pages
        this.openPages = new ArrayList<Integer>();

        //create first node
        bufferManager.addPage(pageName, openPages);
        Node rootNode = new Node(false, -1, keyType, 0);
        bufferManager.writePage(pageName, root, rootNode.toBytes());
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

        //get open pages
        int numOpenPages = buffer.getInt();
        this.openPages = new ArrayList<Integer>();
        for (int i = 0; i < numOpenPages; i++) {
            openPages.add(buffer.getInt());
        }
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
        TreeMap<Object, Integer> temp = currentNode.getPageNums();
        if (temp.size() > size - 2) {
            //TODO split node
        }

        //add values
        temp.put(primaryKeyValue, pageNum);
        currentNode.setPageNums(temp);
        temp = currentNode.getIndexes();
        temp.put(primaryKeyValue, index);
        currentNode.setIndexes(temp);

        bufferManager.writePage(pageName, currentNode.getSelf(), currentNode.toBytes());
    }

    /**
     * Removes the node associated with the primary key from the B+ tree.
     * @param primaryKeyValue The primary key value of the node being removed.
     */
    public void removeFromIndex(Object primaryKeyValue) {
        //get leaf node
        Node currentNode = getToLeafNode(primaryKeyValue);
        TreeMap<Object, Integer> temp = currentNode.getPageNums();

        //remove values
        temp.remove(primaryKeyValue);
        currentNode.setPageNums(temp);
        temp = currentNode.getIndexes();
        temp.remove(primaryKeyValue);
        currentNode.setIndexes(temp);

        //check if compliant
        if (temp.size() < Math.ceil((size - 1)/2) && currentNode.getSelf() != root) {
            underfull(currentNode, primaryKeyValue);
        }

        bufferManager.writePage(pageName, currentNode.getSelf(), currentNode.toBytes());
    }

    /**
     * Splits a node into two when a node is full.
     */
    public void splitNode(Node currentNode, boolean isInternal) {
        //make new node
        int newNum = bufferManager.addPage(pageName, openPages);
        Node newNode = new Node(isInternal, currentNode.getParent(), keyType, newNum);

        //split values between new node and current
        TreeMap<Object, Integer> currentPages = currentNode.getPageNums();
        TreeMap<Object, Integer> currentIndexes = currentNode.getPageNums();
        TreeMap<Object, Integer> newPages = new TreeMap<Object, Integer>();
        TreeMap<Object, Integer> newIndexes = new TreeMap<Object, Integer>();

        while (currentPages.size() > newPages.size()) {
            //get first entry
            Map.Entry<Object, Integer> pageEntry = currentPages.firstEntry();
            Map.Entry<Object, Integer> indexEntry = currentPages.firstEntry();
            //remove from current
            currentPages.remove(pageEntry.getKey());
            currentIndexes.remove(indexEntry.getKey());
            //add to new
            newPages.put(pageEntry.getKey(), pageEntry.getValue());
            newIndexes.put(indexEntry.getKey(), pageEntry.getValue());
        }

        //save new values of current
        currentNode.setPageNums(currentPages);
        currentNode.setIndexes(currentIndexes);
        bufferManager.writePage(pageName, currentNode.getSelf(), currentNode.toBytes());

        //save new values of new
        newNode.setPageNums(newPages);
        newNode.setIndexes(newIndexes);
        bufferManager.writePage(pageName, newNode.getSelf(), newNode.toBytes());

        //if root, create new root
        if (currentNode.getSelf() == root) {

        }
        else {
            //get parent
            byte[] parentBytes = bufferManager.getPage(pageName, currentNode.getParent());
            Node parentNode = new Node(parentBytes, keyType, currentNode.getParent());
            TreeMap<Object, Integer> parentMap = parentNode.getPageNums();
        }

    }

    public int underfull(Node current, Object primKey) {
        Node parent = new Node(bufferManager.getPage(pageName, current.getParent()), keyType, current.getParent());

        if (mergeLeft(current, parent, primKey) != 1){
            if (mergeRight(current, parent, primKey) != 1){
                if (borrowLeft(current, parent, primKey) != 1){
                    if (borrowRight(current, parent, primKey) != 1){
                        return -1;
                    }
                }
            }
        }
        if (parent.getParent() == -1 && parent.getPageNums().size() <= 1){
            //TODO changeRoot();
        }
        if (parent.getPageNums().size() < Math.ceilDiv(size, 2)){
            underfull(parent, primKey);
        }
        return 1;

    }

    /**
     * The first operation that happens when a node is underfull. Will attempt
     * to merge the underfull node with the node to it's left in the B+ tree.
     *
     * @param current The current node that is underfull.
     * @param parent The parent of the underfull node.
     * @param primKey the primarykey that is getting deleted from the B+ tree.
     * @return 1 if success, 0 if failure.
     */
    public int mergeLeft(Node current, Node parent, Object primKey){

        return 0;
    }

    /**
     * The second operation that happens when a node is underfull. Will attempt
     * to merge the underfull node with the node to it's right in the B+ tree.
     *
     * @param current The current node that is underfull.
     * @param parent The parent of the underfull node.
     * @param primKey the primarykey that is getting deleted from the B+ tree.
     * @return 1 if success, 0 if failure.
     */
    public int mergeRight(Node current, Node parent, Object primKey){
        return 0;
    }

    /**
     * The third operation that happens when a node is underfull. Will attempt
     * to borrow a primarykey from the node to it's left in the B+ tree.
     *
     * @param current The current node that is underfull.
     * @param parent The parent of the underfull node.
     * @param primKey the primarykey that is getting deleted from the B+ tree.
     * @return 1 if success, 0 if failure.
     */
    public int borrowLeft(Node current, Node parent, Object primKey){

        return 0;
    }

    /**
     * The foruth and final operation that happens when a node is underfull.
     * Will attempt to borrow a primarykey from the node to it's right in the B+ tree.
     *
     * @param current The current node that is underfull.
     * @param parent The parent of the underfull node.
     * @param primKey the primarykey that is getting deleted from the B+ tree.
     * @return 1 if success, 0 if failure. Should not be possible to fail at this point.
     */
    public int borrowRight(Node current, Node parent, Object primKey){
        return 0;
    }

    public Node getToLeafNode(Object primaryKeyValue) {
        //setup initial
        byte[] nodeBytes = bufferManager.getPage(pageName, root);
        Node currentNode = new Node(nodeBytes, keyType, root);

        //loop through internal
        while (currentNode.isInternal()) {
            for (Map.Entry<Object, Integer> entry : currentNode.getPageNums().entrySet()) {
                Object key = entry.getKey();
                if (lessThan(key, primaryKeyValue)) {
                    nodeBytes = bufferManager.getPage(pageName, entry.getValue());
                    currentNode = new Node(nodeBytes, keyType, entry.getValue());
                    break;
                }
            }
            nodeBytes = bufferManager.getPage(pageName, currentNode.getFinalValue());
            currentNode = new Node(nodeBytes, keyType, currentNode.getFinalValue());
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

        //return if node empty
        if (currentNode.getPageNums().size() < 1) {
            return null;
        }

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


        //get less than


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

        int[] results = new int[2];
        results[0] = currentNode.getPageNums().lastEntry().getValue();
        results[1] = currentNode.getIndexes().lastEntry().getValue() + 1;
        return results;
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

        //set openPages
        buffer.putInt(openPages.size());
        for (int num: openPages) {
            buffer.putInt(num);;
        }

        //return bytes
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
        int size = Integer.BYTES*2;

        //count the open page array
        size += Integer.BYTES;
        for (int num: openPages) {
            size += Integer.BYTES;
        }

        //return
        return size;
    }
}
