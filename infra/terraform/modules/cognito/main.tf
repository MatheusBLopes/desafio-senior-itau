# Autenticação básica com JWT: um user pool e um app client público (sem segredo).
# Sem e-mail, SMS ou Lambda: a API cria o usuário (SignUp) e o confirma na hora (AdminConfirmSignUp),
# com permissão IAM só para essa ação. Tier Lite: grátis até 10 mil usuários ativos por mês.

variable "name" {
  type = string
}

resource "aws_cognito_user_pool" "this" {
  name                = var.name
  user_pool_tier      = "LITE"
  deletion_protection = "INACTIVE" # o ambiente é de demonstração: terraform destroy deve conseguir apagar

  username_configuration {
    case_sensitive = false # "Ana" e "ana" são o mesmo usuário
  }

  password_policy {
    minimum_length                   = 8
    require_lowercase                = true
    require_numbers                  = true
    require_symbols                  = false
    require_uppercase                = false
    temporary_password_validity_days = 1
  }

  # Sem atributo verificado (e-mail/telefone), a recuperação de senha é só pelo administrador.
  account_recovery_setting {
    recovery_mechanism {
      name     = "admin_only"
      priority = 1
    }
  }

  admin_create_user_config {
    allow_admin_create_user_only = false # SignUp público, chamado pela API
  }
}

resource "aws_cognito_user_pool_client" "api" {
  name         = "${var.name}-api"
  user_pool_id = aws_cognito_user_pool.this.id

  generate_secret     = false
  explicit_auth_flows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]

  access_token_validity  = 60
  id_token_validity      = 60
  refresh_token_validity = 1
  token_validity_units {
    access_token  = "minutes"
    id_token      = "minutes"
    refresh_token = "days"
  }

  # Login falho responde igual para usuário inexistente e senha errada (não revela quem está cadastrado).
  prevent_user_existence_errors = "ENABLED"
  enable_token_revocation       = true
}

output "user_pool_id" {
  value = aws_cognito_user_pool.this.id
}

output "user_pool_arn" {
  value = aws_cognito_user_pool.this.arn
}

output "client_id" {
  value = aws_cognito_user_pool_client.api.id
}
