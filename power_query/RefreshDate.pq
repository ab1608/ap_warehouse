let
    // Get current time in UTC with timezone information
    utcNow = DateTimeZone.UtcNow(),
    // Switch the zone to a fixed offset of UTC-5 (EST)
    estTime = DateTimeZone.SwitchZone(utcNow, -5, 0),
    // Optionally remove the timezone information if only the datetime value is needed
    result = DateTimeZone.RemoveZone(estTime),
    #"Converted to Table1" = #table(1, {{result}}),
    #"Changed Type" = Table.TransformColumnTypes(#"Converted to Table1", {{"Column1", type datetime}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type", {{"Column1", "Refresh date"}}),
    #"Added Custom" = Table.AddColumn(
        #"Renamed Columns",
        "Refresh Date Text",
        each DateTime.ToText([Refresh date], [Format = "dd-MMM-yyyy hh:mm tt"])
    ),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom", {{"Refresh Date Text", type text}})
in
    #"Changed Type1"
