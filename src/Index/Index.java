package src.Index;

import com.sun.source.tree.Tree;
import src.StorageManager.BufferManager;
import src.StorageManager.Record;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
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
        int pairSize =+ Integer.BYTES + keySize + Integer.BYTES + Integer.BYTES;
        this.size = (int) (Math.floor((bufferManager.getPageSize() - 20) / pairSize));

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
    public void addToIndex(Object primaryKeyValue, int pageNum, int index, String type) {
        //get leaf node
        Node currentNode = getToLeafNode(primaryKeyValue);
        TreeMap<TreeMapObj, Integer> tempPages = currentNode.getPageNums();
        TreeMap<TreeMapObj, Integer> tempIndexes = currentNode.getIndexes();

        TreeMapObj newVal = new TreeMapObj(type,primaryKeyValue);
        //add values
        tempPages.put(newVal, pageNum);
        currentNode.setPageNums(tempPages);
        tempIndexes.put(newVal, index);
        currentNode.setIndexes(tempIndexes);

        bufferManager.writePage(pageName, currentNode.getSelf(), currentNode.toBytes());

        if (tempPages.size() > size - 1) {
            splitNode(currentNode);
        }
    }

    /**
     * Removes the node associated with the primary key from the B+ tree.
     * @param primaryKeyValue The primary key value of the node being removed.
     */
    public void removeFromIndex(Object primaryKeyValue, String type) {
        //get leaf node
        TreeMapObj treOb = new TreeMapObj(type, primaryKeyValue);
        Node currentNode = getToLeafNode(primaryKeyValue);
        currentNode.getPageNums().remove(treOb);
        currentNode.getIndexes().remove(treOb);
        //check if compliant
        if (currentNode.getPageNums().size() < Math.ceil((size - 1)/2)) {
            underfull(currentNode, primaryKeyValue);
        }

        bufferManager.writePage(pageName, currentNode.getSelf(), currentNode.toBytes());
    }

    /**
     * Splits a node into two when a node is full.
     */
    public void splitNode(Node currentNode) {
        //make new node

        int newNum = bufferManager.addPage(pageName, openPages);
        Node newNode = new Node(currentNode.isInternal(), currentNode.getParent(), keyType, newNum);

        //split values between new node and current
        TreeMap<TreeMapObj, Integer> currentPages = currentNode.getPageNums();
        TreeMap<TreeMapObj, Integer> currentIndexes = currentNode.getIndexes();
        TreeMap<TreeMapObj, Integer> newPages = new TreeMap<TreeMapObj, Integer>();
        TreeMap<TreeMapObj, Integer> newIndexes = new TreeMap<TreeMapObj, Integer>();

        while (currentPages.size() >= newPages.size()) {
            //transfer indexes only when not internal
            if (!currentNode.isInternal()) {
                Map.Entry<TreeMapObj, Integer> indexEntry = currentIndexes.firstEntry();
                currentIndexes.remove(indexEntry.getKey());
                newIndexes.put(indexEntry.getKey(), indexEntry.getValue());
            }
            //get first entry
            Map.Entry<TreeMapObj, Integer> pageEntry = currentPages.firstEntry();


            //remove from current
            currentPages.remove(pageEntry.getKey());

            //add to new
            newPages.put(pageEntry.getKey(), pageEntry.getValue());
        }
        TreeMapObj key = null;
        if(currentNode.isInternal()){
            key = newPages.lastKey();
            newNode.setFinalValue(newPages.get(key));
            newPages.remove(key);
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
        if (currentNode.getSelf() == this.root) {
            int newParentNum = bufferManager.addPage(pageName, openPages);
            Node newParentNode = new Node(true, -1, keyType, newParentNum);
            TreeMap<TreeMapObj, Integer> children = new TreeMap<TreeMapObj, Integer>();
            children.put(currentNode.getPageNums().firstKey(), newNode.getSelf());

            //save values
            newParentNode.setPageNums(children);
            newParentNode.setFinalValue(currentNode.getSelf());

            //save new parent
            bufferManager.writePage(pageName, newParentNode.getSelf(), newParentNode.toBytes());
            this.root = newParentNode.getSelf();

            //add new parent to children
            currentNode.setParent(newParentNum);
            bufferManager.writePage(pageName, currentNode.getSelf(), currentNode.toBytes());
            newNode.setParent(newParentNum);
            bufferManager.writePage(pageName, newNode.getSelf(), newNode.toBytes());
        }
        else {
            //get parent
            byte[] parentBytes = bufferManager.getPage(pageName, currentNode.getParent());
            Node parentNode = new Node(parentBytes, keyType, currentNode.getParent());
            if (currentNode.isInternal()){
                parentNode.getPageNums().put(key, newNode.getSelf());
                bufferManager.writePage(pageName, parentNode.getSelf(), parentNode.toBytes());

            }
            else{
                //add new value to parent
                TreeMap<TreeMapObj, Integer> parentMap = parentNode.getPageNums();
                parentMap.put(currentNode.getPageNums().firstKey(), newNode.getSelf());

                //save parent
                parentNode.setPageNums(parentMap);
                bufferManager.writePage(pageName, parentNode.getSelf(), parentNode.toBytes());

            }
            //check if parent needs splitting
            if (parentNode.getPageNums().size() > size - 1) {
                splitNode(parentNode);
            }

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
        if (parent.getParent() == -1 && parent.getPageNums().size() < 1){
            current.setParent(-1);
            openPages.add(parent.getSelf());
            return 1;
        }
        if (parent.getPageNums().size() < Math.ceilDiv(size, 2)){
            if (parent.getParent() != -1) {
                underfull(parent, primKey);
            }
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
        TreeMapObj left = null;
        TreeMapObj center = null;
        for (TreeMapObj obj : parent.getPageNums().keySet()) {
            if (parent.getPageNums().get(obj) == current.getSelf()){
                center = obj;
                break;
            }
            left = obj;
        }
        if (left == null){
            return -1;
        }

        Node leftNode = new Node(bufferManager.getPage(pageName, parent.getPageNums().get(left)), keyType, parent.getPageNums().get(left));
        if (leftNode.getPageNums().size() + current.getPageNums().size() > size){
            return -2;
        }

        for (TreeMapObj obj : leftNode.getPageNums().keySet()) {
            current.getPageNums().put(obj, leftNode.getPageNums().get(obj));
        }
        if (!current.isInternal()){
            for (TreeMapObj obj : leftNode.getIndexes().keySet()) {
                current.getIndexes().put(obj, leftNode.getIndexes().get(obj));
            }
            openPages.add(leftNode.getSelf());
        }
        else{
            current.getPageNums().put(left, leftNode.getFinalValue());
        }

        parent.getPageNums().remove(left);
        return 1;
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
        TreeMapObj right = null;
        TreeMapObj center = null;
        for (TreeMapObj obj : parent.getPageNums().keySet()) {
            if (parent.getPageNums().get(obj) == current.getSelf()){
                center = obj;
                break;
            }
        }
        if (center == null) {
            return -1;
        }
        for (TreeMapObj obj : parent.getPageNums().keySet()) {
            if (obj.compareTo(center) == 1){
                right = obj;
                break;
            }
        }
        Node rightNode;
        if (right == null) {
            rightNode = new Node(bufferManager.getPage(pageName, parent.getFinalValue()), keyType, parent.getFinalValue());
        }
        else {
            rightNode = new Node(bufferManager.getPage(pageName, parent.getPageNums().get(right)), keyType, parent.getPageNums().get(right));
        }
        if (rightNode.getPageNums().size() + current.getPageNums().size() > size){
            return -2;
        }

        for (TreeMapObj obj : current.getPageNums().keySet()) {
            current.getPageNums().put(obj, current.getPageNums().get(obj));
        }
        if (!current.isInternal()){
            for (TreeMapObj obj : current.getIndexes().keySet()) {
                current.getIndexes().put(obj, current.getIndexes().get(obj));
            }
            openPages.add(current.getSelf());
        }
        else{
            rightNode.getPageNums().put(center, current.getFinalValue());
        }

        parent.getPageNums().remove(center);
        return 1;
    }

    /**
     * The third operation that happens when a node is underfull. Will attempt
     * to borrow a primarykey from the node to it's left in the B+ tree.
     *
     * @param current The current node that is underfull.
     * @param parent The parent of the underfull node.
     * @param primKey the primarykey that is getting deleted from the B+ tree.
     * @return 1 if success, -1 if failure.
     */
    public int borrowLeft(Node current, Node parent, Object primKey){
        TreeMap<TreeMapObj, Integer> parentPageNums = parent.getPageNums();
        TreeMap<TreeMapObj, Integer> currentPages = current.getPageNums();
        TreeMap<TreeMapObj, Integer> currentIndexes = current.getIndexes();
        TreeMapObj previousTMO = null;
        Node previousNode = null;

        for(TreeMapObj tmo : parentPageNums.keySet()){
            if(parentPageNums.get(tmo) == current.getSelf()){
                break;
            }
            else{
                previousTMO = tmo;
            }
        }
        if(previousTMO == null){
            return -1;
        }
        Integer pageNum = parentPageNums.get(previousTMO);
        byte[] nodeBytes = bufferManager.getPage(pageName, pageNum);
        Node currentNode = new Node(nodeBytes, keyType, pageNum);

        TreeMap<TreeMapObj, Integer> previousPages = previousNode.getPageNums();
        TreeMap<TreeMapObj, Integer> previousIndexes = previousNode.getIndexes();

        // cannot borrow if it will underfill previous node
        if (previousPages.size() <= Math.ceil((size - 1)/2)){
            return -1;
        }

        Map.Entry<TreeMapObj, Integer> indexEntry = previousPages.lastEntry();
        previousIndexes.remove(indexEntry.getKey());
        currentIndexes.put(indexEntry.getKey(), indexEntry.getValue());

        //get last entry
        Map.Entry<TreeMapObj, Integer> pageEntry = previousPages.lastEntry();
        //remove from current
        previousPages.remove(pageEntry.getKey());
        //add to new
        currentPages.put(pageEntry.getKey(), pageEntry.getValue());
        
        return 1;
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
        TreeMap<TreeMapObj, Integer> parentPageNums = parent.getPageNums();
        TreeMap<TreeMapObj, Integer> currentPages = current.getPageNums();
        TreeMap<TreeMapObj, Integer> currentIndexes = current.getIndexes();
        TreeMapObj nextTMO = null;
        Node nextNode = null;
        boolean found = false;

        for(TreeMapObj tmo : parentPageNums.keySet()){
            if (found){
                nextTMO = tmo;
                break;
            }
            if(parentPageNums.get(tmo) == current.getSelf()){
                found = true;
            }

        }
        if(nextTMO == null){
            return -1;
        }
        Integer pageNum = parentPageNums.get(nextTMO);
        byte[] nodeBytes = bufferManager.getPage(pageName, pageNum);
        Node currentNode = new Node(nodeBytes, keyType, pageNum);

        TreeMap<TreeMapObj, Integer> nextPages = nextNode.getPageNums();
        TreeMap<TreeMapObj, Integer> nextIndexes = nextNode.getIndexes();

        // cannot borrow if it will underfill previous node
        if (nextPages.size() <= Math.ceil((size - 1)/2)){
            return -1;
        }

        Map.Entry<TreeMapObj, Integer> indexEntry = nextPages.firstEntry();
        nextIndexes.remove(indexEntry.getKey());
        currentIndexes.put(indexEntry.getKey(), indexEntry.getValue());

        //get last entry
        Map.Entry<TreeMapObj, Integer> pageEntry = nextPages.firstEntry();
        //remove from current
        nextPages.remove(pageEntry.getKey());
        //add to new
        currentPages.put(pageEntry.getKey(), pageEntry.getValue());

        return 1;
    }

    public Node getToLeafNode(Object primaryKeyValue) {
        //setup initial
        byte[] nodeBytes = bufferManager.getPage(pageName, root);
        Node currentNode = new Node(nodeBytes, keyType, root);

        //loop through internal
        while (currentNode.isInternal()) {
            boolean found = false;
            for (Map.Entry<TreeMapObj, Integer> entry : currentNode.getPageNums().entrySet()) {
                Object key = entry.getKey().getPrimaryKeyValue();
                if (lessThan(primaryKeyValue, key)) {
                    nodeBytes = bufferManager.getPage(pageName, entry.getValue());
                    currentNode = new Node(nodeBytes, keyType, entry.getValue());
                    found = true;
                    break;
                }
            }
            if(!found){
                nodeBytes = bufferManager.getPage(pageName, currentNode.getFinalValue());
                currentNode = new Node(nodeBytes, keyType, currentNode.getFinalValue());
            }
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

        for (Map.Entry<TreeMapObj, Integer> entry : currentNode.getPageNums().entrySet()) {
            Object primaryKey = entry.getKey().getPrimaryKeyValue();
            Object TreeMapKey = entry.getKey();
            if (equals(primaryKey, primaryKeyValue)) {
                int[] results = new int[2];
                results[0] = currentNode.getPageNums().get(TreeMapKey);
                results[1] = currentNode.getIndexes().get(TreeMapKey);
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


        for (Map.Entry<TreeMapObj, Integer> entry : currentNode.getPageNums().entrySet()) {
            Object primaryKey = entry.getKey().getPrimaryKeyValue();
            Object key = entry.getKey();
            if (equals(primaryKey, primaryKeyValue)) {
                return null;
            }
            if (lessThan(primaryKeyValue, primaryKey)) {
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
            String type = records.get(i).getKeyType();

            update(key, pageNumber, i, type);
        }
    }

    private void update(Object primaryKeyValue, int page, int index, String type) {
        Node currentNode = getToLeafNode(primaryKeyValue);
        TreeMap<TreeMapObj, Integer> pages = currentNode.getPageNums();
        TreeMap<TreeMapObj, Integer> indexes = currentNode.getIndexes();
        //set values
        TreeMapObj toBeAdded = new TreeMapObj(type, primaryKeyValue);
        pages.put(toBeAdded, page);
        indexes.put(toBeAdded, index);
        //save values
        currentNode.setIndexes(indexes);
        currentNode.setPageNums(pages);
        bufferManager.writePage(pageName, currentNode.getSelf(), currentNode.toBytes());
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
            e.printStackTrace();
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
        catch (Exception e) {
            e.printStackTrace();
        }
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
