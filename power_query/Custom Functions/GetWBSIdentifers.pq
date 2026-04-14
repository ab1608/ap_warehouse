(SourceTable as table) as table =>
    let
        #"Merged Queries" = Table.NestedJoin(
            SourceTable, {"WBS Element Code"}, #"WBS Elements", {"WBS Element Code"}, "WBS Elements",
            JoinKind.LeftOuter
        ),
        #"Expand WBS Profit Center and WBS GL" = Table.ExpandTableColumn(
            #"Merged Queries",
            "WBS Elements",
            {"WBS G/L Account", "WBS Profit Center Code", "WBS Parent Code", "WBS Type Char", "WBS Type Local"},
            {"WBS G/L Account", "WBS Profit Center Code", "WBS Parent Code", "WBS Type Char", "WBS Type Local"}
        ),
        #"Copy G/L Account" = Table.AddColumn(
            #"Expand WBS Profit Center and WBS GL", "Native G/L Account", each [#"G/L Account"]
        ),
        #"Copy Profit Center Code" = Table.AddColumn(
            #"Copy G/L Account", "Native Profit Center Code", each [Profit Center Code]
        ),
        #"Drop Original Profit Center and GL" = Table.RemoveColumns(
            #"Copy Profit Center Code", {"G/L Account", "Profit Center Code"}
        ),
        #"Fill G/L Accounts" = Table.AddColumn(
            #"Drop Original Profit Center and GL",
            "G/L Account",
            each
                if ([#"WBS G/L Account"] is null or [#"WBS G/L Account"] = "") then
                    [#"Native G/L Account"]
                else
                    [#"WBS G/L Account"]
        ),
        #"Fill Profit Center Codes" = Table.AddColumn(
            #"Fill G/L Accounts",
            "Profit Center Code",
            each
                if ([#"WBS Profit Center Code"] is null or [#"WBS Profit Center Code"] = "") then
                    [#"Native Profit Center Code"]
                else
                    [#"WBS Profit Center Code"]
        )
    in
        #"Fill Profit Center Codes"
