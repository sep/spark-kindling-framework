provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
  tenant_id       = var.azure_tenant_id
  environment     = var.azure_environment
}

resource "azurerm_resource_group" "eventhub" {
  count    = var.create_resource_group ? 1 : 0
  name     = var.resource_group_name
  location = var.resource_group_location
}

data "azurerm_resource_group" "eventhub" {
  count = var.create_resource_group ? 0 : 1
  name  = var.resource_group_name
}

locals {
  eventhub_resource_group_name     = var.create_resource_group ? azurerm_resource_group.eventhub[0].name : data.azurerm_resource_group.eventhub[0].name
  eventhub_resource_group_location = var.create_resource_group ? azurerm_resource_group.eventhub[0].location : data.azurerm_resource_group.eventhub[0].location
  create_consumer_group            = var.consumer_group_name != "$Default"
}

resource "azurerm_eventhub_namespace" "test" {
  name                = var.namespace_name
  location            = local.eventhub_resource_group_location
  resource_group_name = local.eventhub_resource_group_name
  sku                 = var.namespace_sku
  capacity            = var.namespace_capacity

  auto_inflate_enabled     = var.namespace_auto_inflate_enabled
  maximum_throughput_units = var.namespace_auto_inflate_enabled ? var.namespace_maximum_throughput_units : 0
}

resource "azurerm_eventhub" "test" {
  name                = var.eventhub_name
  namespace_name      = azurerm_eventhub_namespace.test.name
  resource_group_name = local.eventhub_resource_group_name
  partition_count     = var.eventhub_partition_count
  message_retention   = var.eventhub_message_retention
}

resource "azurerm_eventhub_authorization_rule" "test" {
  name                = var.authorization_rule_name
  namespace_name      = azurerm_eventhub_namespace.test.name
  eventhub_name       = azurerm_eventhub.test.name
  resource_group_name = local.eventhub_resource_group_name

  listen = var.authorization_rule_listen
  send   = var.authorization_rule_send
  manage = var.authorization_rule_manage
}

resource "azurerm_eventhub_consumer_group" "test" {
  count               = local.create_consumer_group ? 1 : 0
  name                = var.consumer_group_name
  namespace_name      = azurerm_eventhub_namespace.test.name
  eventhub_name       = azurerm_eventhub.test.name
  resource_group_name = local.eventhub_resource_group_name
}
