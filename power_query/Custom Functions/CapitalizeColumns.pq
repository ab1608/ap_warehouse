(SourceTable as table) as table =>
    let
        CapitalizeAllColumns = Table.TransformColumnNames(SourceTable, Text.Upper),
        StripWhiteSpace = Table.TransformColumnNames(CapitalizeAllColumns, Text.Trim),
        CleanWhitespace = Table.TransformColumnNames(StripWhiteSpace, Text.Clean)
    in
        CleanWhitespace
