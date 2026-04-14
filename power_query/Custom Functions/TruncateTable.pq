(SourceTable as table) as table =>
    let
        TruncateTable = if Truncate = "Yes" then Table.FirstN(SourceTable, 10000) else SourceTable
    in
        TruncateTable
