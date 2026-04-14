let
    Source = Table.Combine({#"Current Actuals - Appended", #"Archived Actuals - Appended"}),
    UseDevMode = UseDevMode(Source),
    #"Added Origin" = Table.AddColumn(UseDevMode, "Origin", each "Actual"),
    #"Cast Data Types" = CastDataTypes(#"Added Origin"),
    #"Rename Source Data" = RenameSourceData(#"Cast Data Types"),
    #"Inverse Amount" = InverseAmount(#"Rename Source Data"),
    #"Get WBS Attributes" = GetWBSIdentifers(#"Inverse Amount"),
    #"Merge GL to Compass Code" = Table.NestedJoin(
        #"Get WBS Attributes",
        {"G/L Account"},
        #"G/L Accounts to Compass Code",
        {"G/L Account"},
        "G/L Accounts to Compass Code",
        JoinKind.LeftOuter
    ),
    #"Expanded G/L Accounts to Compass Code" = Table.ExpandTableColumn(
        #"Merge GL to Compass Code", "G/L Accounts to Compass Code", {"Compass Code"}, {"Compass Code"}
    ),
    #"Get PC Signatures" = GetPCSignatures(#"Expanded G/L Accounts to Compass Code"),
    #"Impute Cost Center" = Table.AddColumn(
        #"Get PC Signatures",
        "Cost Center Code New",
        each if [Cost Center Code] = "" then [Partner Cost Center Code] else [Cost Center Code]
    ),
    #"Drop Original Cost Center Code" = Table.RemoveColumns(#"Impute Cost Center", {"Cost Center Code"}),
    #"Rename as Cost Center Code" = Table.RenameColumns(
        #"Drop Original Cost Center Code", {{"Cost Center Code New", "Cost Center Code"}}
    ),
    #"Changed CCC to Text" = Table.TransformColumnTypes(
        #"Rename as Cost Center Code", {{"Cost Center Code", type text}}
    ),
    #"Determine Fiscal Type" = DetermineSpendType(#"Changed CCC to Text"),
    #"Filter non-M WBS Elements" = Table.SelectRows(#"Determine Fiscal Type", each ([WBS Type Char] <> "M")),
    #"Filter for M WBS Elements" = Table.SelectRows(#"Determine Fiscal Type", each ([WBS Type Char] = "M")),
    #"Copy WBS Elements" = Table.AddColumn(
        #"Filter for M WBS Elements", "WBS Element Code Temp", each [WBS Element Code]
    ),
    #"Remove WBS Elements" = Table.RemoveColumns(#"Copy WBS Elements", {"WBS Element Code"}),
    #"Introduce blank WBS Elements" = Table.TransformColumnTypes(
        Table.AddColumn(#"Remove WBS Elements", "WBS Element Code", each ""), {{"WBS Element Code", type text}}
    ),
    #"Redetermine Fiscal Types for M WBS" = DetermineSpendType(#"Introduce blank WBS Elements"),
    #"Removed blank WBS Column" = Table.RemoveColumns(#"Redetermine Fiscal Types for M WBS", {"WBS Element Code"}),
    #"Rename temp as WBS Element" = Table.RenameColumns(
        #"Removed blank WBS Column", {{"WBS Element Code Temp", "WBS Element Code"}}
    ),
    #"Appended M and non-M WBS Elements" = Table.Combine(
        {#"Rename temp as WBS Element", #"Filter non-M WBS Elements"}
    )
in
    #"Appended M and non-M WBS Elements"
