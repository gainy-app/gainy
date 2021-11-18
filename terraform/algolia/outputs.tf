output "algolia_tickers_index" {
  value = algolia_index.tickers_index.name
}

output "algolia_collections_index" {
  value = algolia_index.collections_index.name
}

output "algolia_indexing_key" {
  value = algolia_api_key.search_key.key
  sensitive = true
}

output "algolia_search_key" {
  value = algolia_api_key.search_key.key
  sensitive = true
}