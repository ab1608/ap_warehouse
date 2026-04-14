(SourceTable as table) as table =>
    let
        #"Label File Type" = Table.AddColumn(
            SourceTable,
            "Data Type",
            each
                if Text.Contains([Name], "CCDet") then
                    "Cost Center Details"
                else if Text.Contains([Name], "Commit_CC") then
                    "Commit CC"
                else if Text.Contains([Name], "Commit_WBS") then
                    "Commit WBS"
                else if Text.Contains([Name], "WBS_Budget") then
                    "WBS Budget"
                else if Text.Contains([Name], "Net Sales") then
                    "Net Sales"
                else
                    "Actual"
        ),
        #"As Text" = Table.TransformColumnTypes(#"Label File Type", {{"Data Type", type text}})
    in
        #"As Text"
