package src.Commands;

public enum NodeType{
    // and or or
    OPERATOR,
    // =, <, >,...
    COMPARATOR,
    // either a column or a value
    // will always be a leaf node
    VALUE
}

