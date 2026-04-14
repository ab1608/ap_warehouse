(SourceTable as table) as table =>
    let
        #"Inverse Sign for Amount" = Table.AddColumn(
            SourceTable, "Amount Negative", each [Amount in Company Code Currency] * -1
        ),
        #"Change to Currency Type" = Table.TransformColumnTypes(
            #"Inverse Sign for Amount", {{"Amount Negative", Currency.Type}}
        ),
        #"Remove Original Amount" = Table.RemoveColumns(
            #"Change to Currency Type", {"Amount in Company Code Currency"}
        ),
        #"Rename as Amount" = Table.RenameColumns(
            #"Remove Original Amount", {{"Amount Negative", "Amount in Company Code Currency"}}
        )
    in
        #"Rename as Amount"
