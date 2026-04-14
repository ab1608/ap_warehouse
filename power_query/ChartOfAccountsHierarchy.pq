let
    Source = #"Chart of Accounts - Base Logic",
    #"Merged Level 1" = Table.NestedJoin(
        Source, {"P&L Line Text"}, #"COA - Level 1", {"P&L Line Text"}, "COA - Level 1", JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 1" = Table.ExpandTableColumn(
        #"Merged Level 1", "COA - Level 1", {"FS ID - FS Text 1", "Order 1"}, {"FS ID - FS Text 1", "Order 1"}
    ),
    #"Merged Level 2" = Table.NestedJoin(
        #"Expanded COA - Level 1",
        {"P&L Line Text"},
        #"COA - Level 2",
        {"P&L Line Text"},
        "COA - Level 2",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 2" = Table.ExpandTableColumn(
        #"Merged Level 2", "COA - Level 2", {"FS ID - FS Text 2", "Order 2"}, {"FS ID - FS Text 2", "Order 2"}
    ),
    #"Merged Level 3" = Table.NestedJoin(
        #"Expanded COA - Level 2",
        {"P&L Line Text"},
        #"COA - Level 3",
        {"P&L Line Text"},
        "COA - Level 3",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 3" = Table.ExpandTableColumn(
        #"Merged Level 3", "COA - Level 3", {"FS ID - FS Text 3", "Order 3"}, {"FS ID - FS Text 3", "Order 3"}
    ),
    #"Merged Level 4" = Table.NestedJoin(
        #"Expanded COA - Level 3",
        {"P&L Line Text"},
        #"COA - Level 4",
        {"P&L Line Text"},
        "COA - Level 4",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 4" = Table.ExpandTableColumn(
        #"Merged Level 4", "COA - Level 4", {"FS ID - FS Text 4", "Order 4"}, {"FS ID - FS Text 4", "Order 4"}
    ),
    #"Merged Level 5" = Table.NestedJoin(
        #"Expanded COA - Level 4",
        {"P&L Line Text"},
        #"COA - Level 5",
        {"P&L Line Text"},
        "COA - Level 5",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 5" = Table.ExpandTableColumn(
        #"Merged Level 5", "COA - Level 5", {"FS ID - FS Text 5", "Order 5"}, {"FS ID - FS Text 5", "Order 5"}
    ),
    #"Merged Level 6" = Table.NestedJoin(
        #"Expanded COA - Level 5",
        {"P&L Line Text"},
        #"COA - Level 6",
        {"P&L Line Text"},
        "COA - Level 6",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 6" = Table.ExpandTableColumn(
        #"Merged Level 6", "COA - Level 6", {"FS ID - FS Text 6", "Order 6"}, {"FS ID - FS Text 6", "Order 6"}
    ),
    #"Merged Level 7" = Table.NestedJoin(
        #"Expanded COA - Level 6",
        {"Level 7 Text"},
        #"COA - Level 7",
        {"P&L Line Text"},
        "COA - Level 7",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 7" = Table.ExpandTableColumn(
        #"Merged Level 7", "COA - Level 7", {"FS ID - FS Text 7", "Order 7"}, {"FS ID - FS Text 7", "Order 7"}
    ),
    #"Merged Level 8" = Table.NestedJoin(
        #"Expanded COA - Level 7",
        {"Level 8 Text"},
        #"COA - Level 8",
        {"P&L Line Text"},
        "COA - Level 8",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 8" = Table.ExpandTableColumn(
        #"Merged Level 8", "COA - Level 8", {"FS ID - FS Text 8", "Order 8"}, {"FS ID - FS Text 8", "Order 8"}
    ),
    #"Merged Level 9" = Table.NestedJoin(
        #"Expanded COA - Level 8",
        {"Level 9 Text"},
        #"COA - Level 9",
        {"P&L Line Text"},
        "COA - Level 9",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 9" = Table.ExpandTableColumn(
        #"Merged Level 9", "COA - Level 9", {"FS ID - FS Text 9", "Order 9"}, {"FS ID - FS Text 9", "Order 9"}
    ),
    #"Merged Level 10" = Table.NestedJoin(
        #"Expanded COA - Level 9",
        {"Level 10 Text"},
        #"COA - Level 10",
        {"P&L Line Text"},
        "COA - Level 10",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 10" = Table.ExpandTableColumn(
        #"Merged Level 10", "COA - Level 10", {"FS ID - FS Text 10", "Order 10"}, {"FS ID - FS Text 10", "Order 10"}
    ),
    #"Merged Level 11" = Table.NestedJoin(
        #"Expanded COA - Level 10",
        {"Level 11 Text"},
        #"COA - Level 11",
        {"P&L Line Text"},
        "COA - Level 11",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 11" = Table.ExpandTableColumn(
        #"Merged Level 11", "COA - Level 11", {"FS ID - FS Text 11", "Order 11"}, {"FS ID - FS Text 11", "Order 11"}
    ),
    #"Merged Level 12" = Table.NestedJoin(
        #"Expanded COA - Level 11",
        {"Level 12 Text"},
        #"COA - Level 12",
        {"P&L Line Text"},
        "COA - Level 12",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 12" = Table.ExpandTableColumn(
        #"Merged Level 12", "COA - Level 12", {"FS ID - FS Text 12", "Order 12"}, {"FS ID - FS Text 12", "Order 12"}
    ),
    #"Merged Level 13" = Table.NestedJoin(
        #"Expanded COA - Level 12",
        {"Level 13 Text"},
        #"COA - Level 13",
        {"P&L Line Text"},
        "COA - Level 13",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 13" = Table.ExpandTableColumn(
        #"Merged Level 13", "COA - Level 13", {"FS ID - FS Text 13", "Order 13"}, {"FS ID - FS Text 13", "Order 13"}
    ),
    #"Merged Level 14" = Table.NestedJoin(
        #"Expanded COA - Level 13",
        {"Level 14 Text"},
        #"COA - Level 14",
        {"P&L Line Text"},
        "COA - Level 14",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 14" = Table.ExpandTableColumn(
        #"Merged Level 14", "COA - Level 14", {"FS ID - FS Text 14", "Order 14"}, {"FS ID - FS Text 14", "Order 14"}
    ),
    #"Merged Level 15" = Table.NestedJoin(
        #"Expanded COA - Level 14",
        {"Level 15 Text"},
        #"COA - Level 15",
        {"P&L Line Text"},
        "COA - Level 15",
        JoinKind.LeftOuter
    ),
    #"Expanded COA - Level 15" = Table.ExpandTableColumn(
        #"Merged Level 15", "COA - Level 15", {"FS ID - FS Text 15", "Order 15"}, {"FS ID - FS Text 15", "Order 15"}
    ),
    #"Removed Other Columns" = Table.SelectColumns(
        #"Expanded COA - Level 15",
        {
            "Financial Statement Item",
            "P&L Line Text",
            "FS Item - P&L Line Text",
            "FS ID - FS Text 1",
            "Order 1",
            "FS ID - FS Text 2",
            "Order 2",
            "FS ID - FS Text 3",
            "Order 3",
            "FS ID - FS Text 4",
            "Order 4",
            "FS ID - FS Text 5",
            "Order 5",
            "FS ID - FS Text 6",
            "Order 6",
            "FS ID - FS Text 7",
            "Order 7",
            "FS ID - FS Text 8",
            "Order 8",
            "FS ID - FS Text 9",
            "Order 9",
            "FS ID - FS Text 10",
            "Order 10",
            "FS ID - FS Text 11",
            "Order 11",
            "FS ID - FS Text 12",
            "Order 12",
            "FS ID - FS Text 13",
            "Order 13",
            "FS ID - FS Text 14",
            "Order 14",
            "FS ID - FS Text 15",
            "Order 15"
        }
    ),
    #"Removed Duplicates" = Table.Distinct(#"Removed Other Columns", {"Financial Statement Item"})
in
    #"Removed Duplicates"
