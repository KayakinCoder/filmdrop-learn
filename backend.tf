terraform {
  backend "s3" {
    bucket       = "fd-jai-tf-state-39204849231"
    key          = "terraform.tfstate"
    region       = "us-west-2"
    use_lockfile = true
  }
}
