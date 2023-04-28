package src.Index;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Node {

    private boolean internal;
    private Integer parent;
    private Map<Object, Integer> pageNums;
    private Map<Object, Integer> indexes;
    private Integer finalValue;
    private String keyType;

    public Node(boolean isInternal, Integer parent, String primaryKeyType) {
        //TODO make a new node
        pageNums = new HashMap<Object, Integer>();
        indexes = new HashMap<Object, Integer>();
        finalValue = null;
        this.internal = isInternal;
        this.parent = parent;
    }

    public Node(byte[] bytes, String primaryKeyType) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

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

        //get pageNums
        this.pageNums = new HashMap<Object, Integer>();
        for (int i = 0; i < numValues; i++) {
            //TODO
            Object key = convertFromBytes(buffer);
            int value = buffer.getInt();
            pageNums.put(key, value);
        }

        //get number of indexes
        numValues = buffer.getInt();

        //get indexes
        this.indexes = new HashMap<Object, Integer>();
        for (int i = 0; i < numValues; i++) {
            //TODO
            Object key = convertFromBytes(buffer);
            int value = buffer.getInt();
            indexes.put(key, value);
        }

        //get final value
        this.finalValue = buffer.getInt();
    }

    public Map<Object, Integer> getPageNums() {
        return pageNums;
    }

    public void setPageNums(Map<Object, Integer> pageNums) {
        this.pageNums = pageNums;
    }

    public Map<Object, Integer> getIndexes() {
        return pageNums;
    }

    public void setIndexes(Map<Object, Integer> indexes) {
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
        for (Map.Entry<Object, Integer> entry : pageNums.entrySet()) {
            //set key
            buffer.put(convertToBytes(entry.getKey()));
            //set page num
            buffer.putInt(entry.getValue());
        }

        //set number of pageNums
        buffer.putInt(indexes.size());

        //set indexes
        for (Map.Entry<Object, Integer> entry : indexes.entrySet()) {
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

        //go through entire file
        for (Map.Entry<Object, Integer> entry : this.getPageNums().entrySet()) {
            size += convertToBytes(entry.getKey()).length;
            size += Integer.BYTES;
        }

        //add final value
        if (finalValue != null) {
            size += Integer.BYTES;
        }

        //return size
        return size;
    }
}
