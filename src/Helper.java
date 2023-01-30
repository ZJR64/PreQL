package src;

import java.io.File;

public class Helper {
    /**
     * Determines if file exists or not.
     *
     * @param file the path to the file.
     */
    public static boolean checkFile(String file) {
        File newFile = new File(file);
        return newFile.exists();
    }

    /**
     * Determines if directory exists or not.
     * If directory does not exist attempt to create one.
     *
     * @param dir the path to the file.
     */
    public static boolean checkDir(String dir) {
        File newDir = new File(dir);
        if (newDir.exists()) {
            return true;
        }
        //create new directory
        try {
            return newDir.mkdir();
        }
        catch(Exception e) {
            return false;
        }
    }

    /**
     * Checks to see if the given string is a num.
     *
     * @param num the num to be tested.
     */
    public static boolean isNum(String num) {
        try {
            Integer.parseInt(num);
            return true;
        }
        catch (NumberFormatException e) {
            return false;
        }
    }
}
