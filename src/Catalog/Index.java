package src.Catalog;

public class Index {

    public Index(int pageSize) {
        //TODO constructor for index
    }

    public void addIndex(Object primaryKeyValue) {
        //TODO add an index to the tree
    }

    public void removeIndex(Object primaryKeyValue) {
        //TODO remove an index from the tree
    }

    public int findIndex(Object primaryKeyValue) {
        //TODO find the page of the given primaryKey
        return -1;
    }

    public byte[] toBytes(){
        //TODO convert to byte array
        byte[] no = new byte[2];
        return no;
    }

    public int getIndexByteSize() {
        //TODO return the number of bytes needed for the index
        return 0;
    }
}
