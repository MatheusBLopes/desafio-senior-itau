variable "name" {
  type = string
}

variable "cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "app_port" {
  type    = number
  default = 8080
}
