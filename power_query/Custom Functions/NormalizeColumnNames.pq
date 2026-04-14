(RawTable as table) as table =>
    let
        NormalColumnNames = Table.RenameColumns(
            RawTable,
            {
                {"Cost Center", "Cost Center Code"},
                {"Cost element", "G/L Account"},
                {"G/L Account Long Name", "G/L Account Name"},
                {"Material", "Material Code"},
                {"Partner Cost Center", "Partner Cost Center Code"},
                {"Profit Center", "Profit Center Code"},
                {"Product", "Product Code"},
                {"Product Description", "Product Name"},
                {"Project External ID", "Project Code"},
                {"Period", "Fiscal Period"},
                {"Signature", "Signature Code"},
                {"Value in Obj. Crcy", "Amount in Company Code Currency"},
                {"WBS Element", "WBS Element Code"},
                {"WBS Element External ID", "WBS Element Code"},
                {"WBS element", "WBS Element Code"}
            },
            MissingField.Ignore
        )
    in
        NormalColumnNames
