(SourceTable as table) as table =>
    let
        // Remove Fiscal Type column if it exists
        RemoveExisting =
            if Table.HasColumns(SourceTable, "Fiscal Type") then
                Table.RemoveColumns(SourceTable, {"Fiscal Type"})
            else
                SourceTable,
        // Add Fiscal Type column with conditional logic
        AddFiscalType = Table.AddColumn(
            RemoveExisting,
            "Fiscal Type",
            each
                if ([WBS Element Code] <> "") then
                    "WBS"
                else if ([Cost Center Code] <> "" or [Partner Cost Center Code] <> "") then
                    "COST CENTER"
                else if ([WBS Element Code] = "" and [Product Code] <> "") then
                    "NO WBS"
                else
                    "OTHER COSTS",
            type text
        )
    in
        AddFiscalType
