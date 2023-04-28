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

    public Index(BufferManager bufferManager, String tableName, String keyType) {
        this.bufferManager = bufferManager;
        root = 0;
        this.pageName = tableName + ".idx";
        this.keyType = keyType;
    }

    public Index(BufferManager bufferManager, String tableName, String keyType, ByteBuffer buffer) {
        this.bufferManager = bufferManager;
        this.root = buffer.getInt();
        this.pageName = tableName + ".idx";
        this.keyType = keyType;
    }

    public void addToIndex(Object primaryKeyValue) {
        //TODO add to the tree
    }

    public void removeFromIndex(Object primaryKeyValue) {
        //TODO remove from the tree
    }

    public int[] find(Object primaryKeyValue) {
        //setup initial
        Node parentNode = null;
        byte[] nodeBytes = bufferManager.getPage(pageName, root);
        Node currentNode = new Node(false);

        while (currentNode.isInternal()) {
            for (Map.Entry<Object, Integer> entry : currentNode.getPageNums().entrySet()) {
                Object key = entry.getKey();
                if (lessThan(key, primaryKeyValue)) {

                }
            }
        }
        return null;
    }

    public void updateIndex(ArrayList<Record> records, int pageNumber) {
        //TODO update the index of whole page
    }

    public byte[] toBytes(){
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getIndexByteSize()]);
        buffer.putInt(root);
        return buffer.array();
    }

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

    public int getIndexByteSize() {
        return Integer.BYTES;
    }
}
