output "eventhub_test_resource_group" {
  description = "Event Hub resource group for system tests"
  value       = local.eventhub_resource_group_name
}

output "eventhub_test_namespace" {
  description = "Event Hubs namespace name for system tests"
  value       = azurerm_eventhub_namespace.test.name
}

output "eventhub_test_name" {
  description = "Event Hub name for system tests"
  value       = azurerm_eventhub.test.name
}

output "eventhub_test_auth_rule" {
  description = "Event Hub authorization rule name for system tests"
  value       = azurerm_eventhub_authorization_rule.test.name
}

output "eventhub_test_consumer_group" {
  description = "Consumer group used by system tests"
  value       = local.create_consumer_group ? azurerm_eventhub_consumer_group.test[0].name : "$Default"
}

output "eventhub_test_connection_string" {
  description = "Primary Event Hub connection string for provider tests"
  value       = azurerm_eventhub_authorization_rule.test.primary_connection_string
  sensitive   = true
}
