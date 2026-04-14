let
    Source = Table.Combine({#"Actuals, Commited, Cost Center Details", #"Forecast - Combined"}),
    #"Changed Type" = Table.TransformColumnTypes(
        Source,
        {
            {"Fiscal Year", Int64.Type},
            {"Amount in Company Code Currency", Currency.Type},
            {"G/L Account", Int64.Type},
            {"Compass Code", type text},
            {"WBS Element Code", type text},
            {"Signature Code", type text},
            {"Origin", type text},
            {"Company Code", type text}
        }
    ),
    #"Fill null Amounts with 0" = Table.ReplaceValue(
        #"Changed Type", null, 0, Replacer.ReplaceValue, {"Amount in Company Code Currency"}
    ),
    #"Merged WBS Budgets on WBS Parent" = Table.NestedJoin(
        #"Fill null Amounts with 0",
        {"WBS Parent Code"},
        #"WBS Budgets",
        {"WBS Parent Code"},
        "WBS Budgets - Complete",
        JoinKind.LeftOuter
    ),
    #"Expanded WBS Budgets - Complete" = Table.ExpandTableColumn(
        #"Merged WBS Budgets on WBS Parent",
        "WBS Budgets - Complete",
        {"Budget released", "Current Budget"},
        {"Budget released", "Current Budget"}
    ),
    #"Renamed Columns" = Table.RenameColumns(
        #"Expanded WBS Budgets - Complete",
        {{"Budget released", "WBS Budget Released"}, {"Current Budget", "WBS Current Budget"}}
    ),
    #"Added Index" = Table.AddIndexColumn(#"Renamed Columns", "Row Index", 1, 1, Int64.Type)
in
    #"Added Index"
