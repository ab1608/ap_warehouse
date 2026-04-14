(SourceTable as table) as table =>
    let
        #"Merged Profit Center Codes" = Table.NestedJoin(
            SourceTable,
            {"Profit Center Code"},
            #"Profit Centers - Stage",
            {"Profit Center Code"},
            "Profit Centers",
            JoinKind.LeftOuter
        ),
        #"Expanded Signature Codes" = Table.ExpandTableColumn(
            #"Merged Profit Center Codes", "Profit Centers", {"Signature Code"}, {"Signature Code"}
        )
    in
        #"Expanded Signature Codes"
