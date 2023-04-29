package src.Index;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Node {

    private boolean internal;
    private Integer parent;
    private Integer self;
    private TreeMap<TreeMapObj, Integer> pageNums;
    private TreeMap<TreeMapObj, Integer> indexes;
    private Integer finalValue;
    private String keyType;

    public Node(boolean isInternal, Integer parent, String primaryKeyType, int self) {
        pageNums = new TreeMap<TreeMapObj, Integer>();
        indexes = new TreeMap<TreeMapObj, Integer>();
        finalValue = -1;
        this.internal = isInternal;
        this.parent = parent;
        this.keyType = primaryKeyType;
        this.self = self;
    }

    public Node(byte[] bytes, String primaryKeyType, int self) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        this.self = self;

        //get isInternal
        int internalNum = buffer.getInt();
        if (internalNum == 0) {
            this.internal = false;
        }
        else {
            this.internal = true;
        }

        //get parent
        this.parent = buffer.getInt();

        //get number of pageNums
        int numValues = buffer.getInt();

        this.keyType = primaryKeyType;

        //get pageNums
        this.pageNums = new TreeMap<TreeMapObj, Integer>();
        for (int i = 0; i < numValues; i++) {
            //TODO
            Object key = convertFromBytes(buffer);
            int value = buffer.getInt();
            TreeMapObj toBePut = new TreeMapObj(primaryKeyType, key);
            pageNums.put(toBePut, value);
        }

        //get number of indexes
        numValues = buffer.getInt();

        //get indexes
        this.indexes = new TreeMap<TreeMapObj, Integer>();
        for (int i = 0; i < numValues; i++) {
            //TODO
            Object key = convertFromBytes(buffer);
            int value = buffer.getInt();
            TreeMapObj toBePut = new TreeMapObj(primaryKeyType, key);
            indexes.put(toBePut, value);
        }

        //get final value
        this.finalValue = buffer.getInt();
    }

    public TreeMap<TreeMapObj, Integer> getPageNums() {
        return pageNums;
    }

    public void setPageNums(TreeMap<TreeMapObj, Integer> pageNums) {
        this.pageNums = pageNums;
    }

    public TreeMap<TreeMapObj, Integer> getIndexes() {
        return pageNums;
    }

    public void setIndexes(TreeMap<TreeMapObj, Integer> indexes) {
        this.indexes = indexes;
    }

    public Integer getFinalValue() {
        return finalValue;
    }

    public void setFinalValue(Integer finalValue) {
        this.finalValue = finalValue;
    }

    public boolean isInternal() {
        return internal;
    }

    public void setInternal(boolean isInternal) {
        this.internal = isInternal;
    }

    public Integer getParent() {
        return parent;
    }

    public void setParent(Integer parent) {
        this.parent = parent;
    }

    public int getSelf() {
        return self;
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getNodeByteSize()]);

        //set isInternal
        if (this.internal) {
            buffer.putInt(1);
        }
        else {
            buffer.putInt(0);
        }

        //set parent
        buffer.putInt(this.parent);

        //set number of pageNums
        buffer.putInt(pageNums.size());

        //set pageNums
        for (Map.Entry<TreeMapObj, Integer> entry : pageNums.entrySet()) {
            //set key
            buffer.put(convertToBytes(entry.getKey()));
            //set page num
            buffer.putInt(entry.getValue());
        }

        //set number of pageNums
        buffer.putInt(indexes.size());

        //set indexes
        for (Map.Entry<TreeMapObj, Integer> entry : indexes.entrySet()) {
            //set key
            buffer.put(convertToBytes(entry.getKey()));
            //set page num
            buffer.putInt(entry.getValue());
        }

        //set final value
        buffer.putInt(finalValue);

        return buffer.array();
    }

    // Convert object to byte[]
    public byte[] convertToBytes(Object keyValue) {
        if (keyType.contains("char")) {
            //string
            String key = (String) keyValue;
            byte[] stringBytes = key.getBytes();
            ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES + stringBytes.length]);
            buffer.putInt(stringBytes.length);
            buffer.put(stringBytes);

            return buffer.array();
        }
        else if (keyType.equalsIgnoreCase("integer")) {
            //integer
            int key = (int) keyValue;

            ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES * 2]);
            buffer.putInt(Integer.BYTES);
            buffer.putInt(key);
            return buffer.array();
        }
        else if (keyType.equalsIgnoreCase("boolean")) {
            //boolean
            boolean key = (boolean) keyValue;

            ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES + Integer.BYTES]);
            buffer.putInt(Integer.BYTES);
            if (key) {
                buffer.putInt(1);
            }
            else {
                buffer.putInt(0);
            }
            return buffer.array();
        }
        else {
            //double
            double key = (double) keyValue;

            ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES + Double.BYTES]);
            buffer.putInt(Double.BYTES);
            buffer.putDouble(key);
            return buffer.array();
        }
    }

    // Convert byte[] to object
    public Object convertFromBytes(ByteBuffer buffer) {
        //get size
        int size = buffer.getInt();

        if (keyType.contains("char")) {
            //string
            byte[] stringArray = new byte[size];
            buffer.get(stringArray);
            String key = new String(stringArray);

            return key;
        }
        else if (keyType.equalsIgnoreCase("integer")) {
            //integer
            int key = buffer.getInt();
            return key;
        }
        else if (keyType.equalsIgnoreCase("boolean")) {
            //boolean
            int key = buffer.getInt();

            if (key == 1) {
                return true;
            }
            else {
                return false;
            }
        }
        else {
            //double
            double key = buffer.getDouble();

            return key;
        }
    }

    public int getNodeByteSize() {
        int size = 0;

        //count isInternal
        size += Integer.BYTES;

        //count parent
        size += Integer.BYTES;

        //count number of pageNums
        size += Integer.BYTES;

        //count pageNums
        for (Map.Entry<TreeMapObj, Integer> entry : pageNums.entrySet()) {
            size += convertToBytes(entry.getKey()).length;
            size += Integer.BYTES;
        }

        //countnumber of pageNums
        size += Integer.BYTES;

        //count indexes
        for (Map.Entry<TreeMapObj, Integer> entry : indexes.entrySet()) {
            size += convertToBytes(entry.getKey()).length;
            size += Integer.BYTES;
        }

        //count final value
        size += Integer.BYTES;


        //return size
        return size;
    }
}
