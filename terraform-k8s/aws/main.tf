
module "networking" {
  source = "./modules/networking"

  vpc_cidr             = var.vpc_cidr
  public_subnets_cidr  = var.public_subnets_cidr
  private_subnets_cidr = var.private_subnets_cidr
  availability_zone    = var.availability_zone
}


module "eks" {
  source = "./modules/eks"

  vpc_id_eks = module.networking.vpc_id
  subnet = module.networking.private_subnets_ids

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  instance_type_group1        = var.instance_type_group1
  instance_type_group2        = var.instance_type_group2
  asg_desired_capacity_group1 = var.asg_desired_capacity_group1
  asg_desired_capacity_group2 = var.asg_desired_capacity_group2
}

# module "ec2" {
#   source = "./modules/ec2"

#   vpc_id_ec2                     = module.networking.vpc_id
#   subnet_id                      = module.networking.public_subnets_ids
  
#   number_of_instances            = var.number_of_instances
#   ec2_name                       = var.ec2_name
#   ec2_security_group_name        = var.ec2_security_group_name
#   ec2_security_group_description = var.ec2_security_group_description
#   ec2_ami                        = var.ec2_ami
#   ec2_instance_type              = var.ec2_instance_type
# }

module "rds" {
  source = "./modules/rds"

  vpc_id_rds        = module.networking.vpc_id
  subnets_rds        = module.networking.private_subnets_ids

  allocated_storage   = var.allocated_storage
  db_engine           = var.db_engine
  db_port             = var.db_port
  engine_version      = var.engine_version
  instance_type       = var.instance_type
  database_name       = var.database_name
  db_username         = var.db_username
  db_password         = var.db_password
  publicly_accessible = var.publicly_accessible
}

# module "s3" {
#   source = "./modules/s3"

#   vpc_id_s3   = module.networking.vpc_id
#   subnet_s3 = module.networking.private_subnets_ids

#   bucket_prefix = var.bucket_prefix
#   acl           = var.acl
#   versioning    = var.versioning
# }

resource "aws_redshift_subnet_group" "redshift_subnet_group"{
  name="redshift-sg"
  subnet_ids = [module.networking.private_subnets_ids[0], 
                module.networking.private_subnets_ids[1] ]
}

resource "aws_redshift_cluster" "myprodcluster" {
  availability_zone = "us-east-2b"
  cluster_identifier = "redshift-cluster-deb"
  cluster_parameter_group_name = "default.redshift-1.0"  
  cluster_type="single-node"
  database_name="dev"
  master_username = "awsuser"
  node_type="dc2.large"
  publicly_accessible = false
  skip_final_snapshot = true
  master_password = "masterRedshift1"
  cluster_subnet_group_name = aws_redshift_subnet_group.redshift_subnet_group.name
  iam_roles = [
   "arn:aws:iam::921884731971:role/redshift_role",
   "arn:aws:iam::921884731971:role/redshift-secrets" 
  ]
  
}

#resource "redshift_schema" "movies_schema"{
#  name="movies_schema"
#  external_schema{
#    database_name="movies_db"
#    data_catalog_source{
#      iam_role_arns=["arn:aws:iam::921884731971:role/redshift_role"]
#      create_external_database_if_not_exists = true
#    }
#  }
#}