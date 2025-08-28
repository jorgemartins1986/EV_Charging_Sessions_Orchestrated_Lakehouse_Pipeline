terraform {
  backend "s3" {
    region       = "eu-central-1"
    profile      = "jorge-martins"
    bucket       = "jorge-1050497-terraform-state-backend"
    key          = "ev-charging-sessions-project/terraform.tfstate"
    encrypt      = true
    use_lockfile = true
  }
}