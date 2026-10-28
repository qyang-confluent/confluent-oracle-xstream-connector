provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      owner_email = "qyang@confluent.io"
    }
  }
}


# Create an EC2 Instance Resource
resource "aws_instance" "qyang_dev" {
  ami           = "ami-0c398cb65a93047f2"
  #ami           = "ami-012ba162b9cd2729c"
  # instance_type = "g5.xlarge" 
   instance_type = "t3.xlarge" 
  #instance_type = "m5.4xlarge" 
  subnet_id = "subnet-023945ae06219d8ca"
  security_groups = ["sg-0b6e033028d61f10d"]
  key_name="qyang-confluent"
  tags = {
    Name        = "qyang-dev"
    Environment = "Dev"
  }

  root_block_device {
    volume_size = 300       # Sets the root volume size to 50 GiB
    volume_type = "gp3"    # Specifies the volume type as General Purpose SSD (gp3)
    delete_on_termination = true
  }

  # Runs once as root on first boot via cloud-init. Output goes to /var/log/user-data.log.
  user_data = <<-EOF
    #!/bin/bash
    set -euxo pipefail
    exec > >(tee -a /var/log/user-data.log) 2>&1

    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y docker.io
    systemctl enable --now docker

    mkdir -p /home/ubuntu/share
    chown ubuntu:ubuntu /home/ubuntu/share
    usermod -aG docker ubuntu

    # Keep a copy of the command for manual re-runs, then execute it.
    # --restart unless-stopped brings the DB back after an instance reboot.
    cat > /home/ubuntu/run_oracledb.sh <<'SCRIPT'
    #!/bin/bash
    docker run -d --name db19 --restart unless-stopped \
      -p 1521:1521 \
      -v /home/ubuntu/share:/share:rw \
      -e ORACLE_SID=orcl -e ORACLE_PDB=orclpdb1 -e ORACLE_PWD=${var.oracle_pwd} \
      -e INIT_SGA_SIZE=4096 -e INIT_PGA_SIZE=1024 \
      -e ENABLE_ARCHIVELOG=true \
      ${var.oracle_image}
    SCRIPT
    chown ubuntu:ubuntu /home/ubuntu/run_oracledb.sh
    chmod 700 /home/ubuntu/run_oracledb.sh

    /home/ubuntu/run_oracledb.sh
  EOF

}

variable "oracle_pwd" {
  type        = string
  description = "Oracle SYS/SYSTEM/PDBADMIN password for the db19 container"
  sensitive   = true
}

variable "oracle_image" {
  type        = string
  description = "Docker image for the Oracle db19 container"
}

output "instance_public_dns" {
  value       = aws_instance.qyang_dev.public_dns
  description = "The public DNS name of the EC2 instance"
}

output "oracle_connection_string" {
  value       = "${aws_instance.qyang_dev.public_dns}:1521:orcl"
  description = "Oracle connection string for the db19 container (hostname:port:service name, CDB service orcl)"
}

