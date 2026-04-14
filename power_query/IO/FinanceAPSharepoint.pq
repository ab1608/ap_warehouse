let
    Source = SharePoint.Contents("https://loreal.sharepoint.com/sites/-US-FinanceAP/", [ApiVersion = 15]),
    #"Documents partages" = Source{[Name = "Documents partages"]}[Content]
in
    #"Documents partages"
