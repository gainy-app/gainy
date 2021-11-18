#################################### Indices ####################################

resource "algolia_index" "tickers_index" {
  name = "${var.env}_tickers"
}

resource "algolia_index" "collections_index" {
  name = "${var.env}_collections"
}


#################################### API keys ####################################

resource "algolia_api_key" "indexing_key" {
  acl         = ["addObject", "deleteObject", "editSettings"]
  description = "[${var.env}] API key for data indexing"
  indexes     = [algolia_index.tickers_index.name, algolia_index.collections_index.name]
}

resource "algolia_api_key" "search_key" {
  acl         = ["search"]
  description = "[${var.env}] API key for search queries"
  indexes     = [algolia_index.tickers_index.name, algolia_index.collections_index.name]
}

