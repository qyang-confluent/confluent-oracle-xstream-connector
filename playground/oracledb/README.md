# qyang-dev EC2 + Oracle 19c

Terraform project that provisions a single personal dev EC2 instance (`aws_instance.qyang_dev`) in `us-east-1`. On first boot the instance installs Docker and starts an Oracle 19c Enterprise Edition container.

## What it creates

- One EC2 instance (`t3.xlarge`, Ubuntu AMI, 300 GiB gp3 root volume), tagged `Name=qyang-dev`, `Environment=Dev`, `owner_email=qyang@confluent.io`.
- Output `instance_public_dns` with the instance's public DNS name.

The AMI, subnet, security group and key pair are hard-coded for the author's AWS account. Changing region means updating all of them.

## Usage

Credentials are loaded from an env file outside the repo (`run.sh` does this):

```
source ~/vault/aws/qyang1.env
terraform init
terraform plan
terraform apply
```

Override the Oracle password (default is set in `main.tf`):

```
export TF_VAR_oracle_pwd='<password>'
# or: terraform apply -var 'oracle_pwd=<password>'
```

Local state is used (`terraform.tfstate`). Use `terraform fmt` / `terraform validate` for checks.

## What happens on first boot

`user_data` runs once as root via cloud-init and:

1. Installs `docker.io` and enables the Docker service.
2. Creates `/home/ubuntu/share` and adds `ubuntu` to the `docker` group.
3. Writes `/home/ubuntu/run_oracledb.sh` and runs it, starting container `db19` from `yangqi2004/oracledb19ee:latest`:
   - port `1521` published
   - `/home/ubuntu/share` mounted at `/share`
   - `ORACLE_SID=orcl`, `ORACLE_PDB=orclpdb1`
   - archivelog enabled, SGA 4096, PGA 1024
   - `--restart unless-stopped`, so it returns after a reboot

The first boot pulls a large image and the database then needs several minutes to initialize.

```
ssh -i <key> ubuntu@<instance_public_dns>
tail -f /var/log/user-data.log   # boot script output
docker logs -f db19              # database startup
```

`user_data` only runs at instance creation. To apply changes to it, recreate the instance (`terraform taint aws_instance.qyang_dev`, or add `user_data_replace_on_change = true`).

## Files

- `main.tf`: provider, instance, `oracle_pwd` variable, output.
- `run_downstream.sh`: not run by Terraform. Manual `docker run` commands for extra containers (`strmdb`, `db19`) to run on the instance after SSH.
- `run.sh`: sources credentials and runs Terraform.

## Notes

- `main.tf` and `run_downstream.sh` contain plaintext Oracle passwords, and the password also ends up in user_data and local state. Don't commit or share these files or the state.
- Instance type is `t3.xlarge`; larger types are left commented out in `main.tf`.
