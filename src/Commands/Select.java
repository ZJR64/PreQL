package src.Commands;

import src.StorageManager.StorageManager;

/**
 * The class for the Select Command.
 * It takes the input as the argument and parses it and executes the command.
 *
 * @author Kaitlyn DeCola kmd8594@rit.edu
 */
public class Select extends Command {
    // table names
    private String[] name;
    private StorageManager sm;
    private WhereClause where;
    private String orderBy;
    private String[] columns;

    // default constructor
    public Select(String input, StorageManager sm){
        super(input);
        this.sm = sm;
    }

      /**
     * parses the command and stores the table name,
     * and if the parse was successful
     */
    @Override
    public void parse(){
        try {
            // initialize where and orderby as null
            this.where = null;
            this.orderBy = null;
            // split on the keyword from
            // should return ["select *", <name>]
            String[] split = input.split("(?i)from");
            String cols = split[0];
            cols = cols.replace("select", "").strip();
            this.columns = cols.split("\\s*,\\s*");
            String splitTable = "";

            // contains where clause
            if(split[1].toLowerCase().contains("where")){
                String[] splitWhere = split[1].split("(?i)where");
                splitTable = splitWhere[0].strip();
                // if there's an orderby, split again, otherwise the rest is the where clause
                if(splitWhere[1].toLowerCase().contains("orderby")){
                    String[] splitOrderBy = splitWhere[1].split("(?i)orderby");
                    this.where = new WhereClause(splitOrderBy[0].strip());
                    this.orderBy = splitOrderBy[1].replace(";","").strip();
                }
                else{
                    this.where = new WhereClause(splitWhere[1].replace(";","").strip());
                }
            }
            // doesn't contain where but contains orderby
            else if(split[1].toLowerCase().contains("orderby")){
                String[] splitOrderBy = split[1].split("(?i)orderby");
                splitTable = splitOrderBy[0].strip();
                this.orderBy = splitOrderBy[1].replace(";","").strip();
            }
            // doesn't contain where or orderby
            else{
                splitTable = split[1].replace(";", "").strip();
            }
            this.name = splitTable.split(", ");
            this.success = true;
        }
        catch(Exception e){
            System.out.println(input + " could not be parsed");
            this.success = false;
        }
    }

    @Override
    public String execute() {
        return null;//sm.getAllRecords(name);
    }
}
