# -----------------------------------------------------------------------------
# Azure
# -----------------------------------------------------------------------------
variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "azure_tenant_id" {
  description = "Azure tenant ID"
  type        = string
  default     = null
  nullable    = true
}

variable "azure_environment" {
  description = "Azure cloud environment for provider (public or usgovernment)"
  type        = string
  default     = "public"

  validation {
    condition     = contains(["public", "usgovernment"], var.azure_environment)
    error_message = "azure_environment must be one of: public, usgovernment."
  }
}

# -----------------------------------------------------------------------------
# Resource Group
# -----------------------------------------------------------------------------
variable "resource_group_name" {
  description = "Resource group for Event Hubs resources"
  type        = string
}

variable "resource_group_location" {
  description = "Location for resource group creation when create_resource_group=true"
  type        = string
  default     = "eastus2"
}

variable "create_resource_group" {
  description = "Whether to create the resource group (false = use existing resource_group_name)"
  type        = bool
  default     = false
}

# -----------------------------------------------------------------------------
# Event Hubs Namespace
# -----------------------------------------------------------------------------
variable "namespace_name" {
  description = "Event Hubs namespace name"
  type        = string
}

variable "namespace_sku" {
  description = "Event Hubs namespace SKU"
  type        = string
  default     = "Standard"

  validation {
    condition     = contains(["Basic", "Standard", "Premium"], var.namespace_sku)
    error_message = "namespace_sku must be one of: Basic, Standard, Premium."
  }
}

variable "namespace_capacity" {
  description = "Namespace throughput units or capacity"
  type        = number
  default     = 1
}

variable "namespace_auto_inflate_enabled" {
  description = "Enable namespace auto-inflate"
  type        = bool
  default     = false
}

variable "namespace_maximum_throughput_units" {
  description = "Maximum throughput units when auto-inflate is enabled"
  type        = number
  default     = 0
}

# -----------------------------------------------------------------------------
# Event Hub
# -----------------------------------------------------------------------------
variable "eventhub_name" {
  description = "Event Hub name"
  type        = string
  default     = "kindling-provider-test-hub"
}

variable "eventhub_partition_count" {
  description = "Partition count for Event Hub"
  type        = number
  default     = 2
}

variable "eventhub_message_retention" {
  description = "Message retention in days for Event Hub"
  type        = number
  default     = 1
}

# -----------------------------------------------------------------------------
# Auth + Consumer Group
# -----------------------------------------------------------------------------
variable "authorization_rule_name" {
  description = "Event Hub authorization rule name"
  type        = string
  default     = "kindling-system-tests"
}

variable "authorization_rule_manage" {
  description = "Allow Manage permission on Event Hub auth rule"
  type        = bool
  default     = true
}

variable "authorization_rule_listen" {
  description = "Allow Listen permission on Event Hub auth rule"
  type        = bool
  default     = true
}

variable "authorization_rule_send" {
  description = "Allow Send permission on Event Hub auth rule"
  type        = bool
  default     = true
}

variable "consumer_group_name" {
  description = "Consumer group name to ensure for tests"
  type        = string
  default     = "$Default"
}
