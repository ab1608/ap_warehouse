let
    Source = #"Extracts - Clean PL",
    #"Unpivoted Other Columns" = Table.UnpivotOtherColumns(
        Source, {"NAME", "FISCAL YEAR", "FISCAL PERIOD", "P&L LINE"}, "Attribute", "Value"
    ),
    #"Renamed Columns" = Table.RenameColumns(
        #"Unpivoted Other Columns", {{"Attribute", "SIGNATURE"}, {"Value", "AMOUNT"}, {"Name", "FILE NAME"}}
    ),
    #"Uppercased Signatures" = Table.TransformColumns(#"Renamed Columns", {{"SIGNATURE", Text.Upper, type text}}),
    #"Trimmed Signatures" = Table.TransformColumns(#"Uppercased Signatures", {{"SIGNATURE", Text.Trim, type text}}),
    #"Cleaned Signatures" = Table.TransformColumns(#"Trimmed Signatures", {{"SIGNATURE", Text.Clean, type text}}),
    #"Replace Non Breaking Space" = Table.ReplaceValue(
        #"Cleaned Signatures", "#(00a0)", " ", Replacer.ReplaceText, {"SIGNATURE"}
    ),
    #"Merged Signatures" = Table.NestedJoin(
        #"Replace Non Breaking Space",
        {"SIGNATURE"},
        #"Signature Codes - FILE",
        {"Signature"},
        "Signature Codes - FILE",
        JoinKind.LeftOuter
    ),
    #"Expanded Signature Codes - FILE" = Table.ExpandTableColumn(
        #"Merged Signatures", "Signature Codes - FILE", {"Division"}, {"Division"}
    ),
    #"Amount as Currency" = Table.TransformColumnTypes(
        #"Expanded Signature Codes - FILE", {{"AMOUNT", Currency.Type}}
    ),
    #"Added Row Index" = Table.AddIndexColumn(#"Amount as Currency", "Index", 1, 1, Int64.Type),
    #"Merge Fiscal Years" = Table.NestedJoin(
        #"Added Row Index", {"FISCAL YEAR"}, #"Fiscal Years - FILE", {"Year"}, "Fiscal Years - FILE",
        JoinKind.LeftOuter
    ),
    #"Expand Year Order" = Table.ExpandTableColumn(
        #"Merge Fiscal Years", "Fiscal Years - FILE", {"Year Order"}, {"Year Order"}
    ),
    #"Create Date" = Table.AddColumn(
        #"Expand Year Order",
        "Date",
        each #date(Number.From([#"FISCAL YEAR"]), Number.From([#"FISCAL PERIOD"]), 1),
        type date
    )
in
    #"Create Date"
