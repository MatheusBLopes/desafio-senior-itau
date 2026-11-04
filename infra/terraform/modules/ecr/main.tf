resource "aws_ecr_repository" "this" {
  name                 = var.name
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true # terraform destroy deixa a conta limpa

  image_scanning_configuration {
    scan_on_push = true
  }
}

# FinOps: mantém só as últimas imagens.
resource "aws_ecr_lifecycle_policy" "this" {
  repository = aws_ecr_repository.this.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Mantem as ultimas ${var.keep_last} imagens"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = var.keep_last
      }
      action = { type = "expire" }
    }]
  })
}

variable "name" {
  type = string
}

variable "keep_last" {
  type    = number
  default = 5
}

output "repository_url" {
  value = aws_ecr_repository.this.repository_url
}

output "repository_arn" {
  value = aws_ecr_repository.this.arn
}
