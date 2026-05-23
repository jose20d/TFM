# ------------------------------------------------------------
# Infraestructura inicial (staging) para TFM GeoContext
# Enfoque: simple, explícito y orientado a aprendizaje.
# ------------------------------------------------------------

# Etiquetas comunes para todos los recursos de este entorno.
locals {
  common_tags = {
    Project     = "TFM"
    Environment = "staging"
  }

  # Elastic IP opcional:
  # - true  => crea y asocia EIP a la EC2
  # - false => no crea EIP
  enable_eip = true
}

# AMI oficial de Ubuntu Server 24.04 LTS (Noble) para us-east-1.
# Owner Canonical: 099720109477
data "aws_ami" "ubuntu_2404" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# 1) VPC base del proyecto.
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(local.common_tags, {
    Name = "tfm-staging-vpc"
  })
}

# 2) Subnet pública donde vivirá la EC2 inicial.
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = merge(local.common_tags, {
    Name = "tfm-staging-public-subnet"
  })
}

# 3) Internet Gateway para salida/entrada a Internet.
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(local.common_tags, {
    Name = "tfm-staging-igw"
  })
}

# 4) Route table pública con ruta por defecto hacia Internet.
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(local.common_tags, {
    Name = "tfm-staging-public-rt"
  })
}

# Asociación de la subnet pública a su route table.
resource "aws_route_table_association" "public_assoc" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# 5) Security Group básico para acceso a la instancia.
#    - SSH (22), HTTP (80), HTTPS (443) desde Internet.
#    - Puertos 3000/8000 abiertos temporalmente para validación de staging
#      hasta publicar por Nginx/HTTPS en 80/443.
resource "aws_security_group" "ec2_basic" {
  name        = "tfm-staging-ec2-sg"
  description = "Acceso basico para EC2 de TFM staging"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Frontend staging temporal"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Backend API staging temporal"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Salida libre"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "tfm-staging-ec2-sg"
  })
}

# 6) Key pair administrado por OpenTofu para acceso SSH.
#    - aws_key_pair registra en AWS la llave pública local.
#    - Se usa file(...) para leer automáticamente el archivo .pub del equipo.
#    - Esto habilita acceso SSH inmediato y deja base lista para automatización futura
#      (bootstrap, configuración remota y pipelines de despliegue).
resource "aws_key_pair" "staging" {
  key_name   = "tfm-staging-key"
  public_key = file(pathexpand("~/.ssh/tfm/tfm-staging.pub"))

  tags = merge(local.common_tags, {
    Name = "tfm-staging-key"
  })
}

# 6) Instancia EC2 Ubuntu 24.04 para staging.
#    Se usa t3.small como mínimo recomendado para soportar build Docker/Next.js
#    sin los límites de RAM/CPU observados en t3.micro.
#    Usa el key pair creado arriba para habilitar SSH seguro.
resource "aws_instance" "app" {
  ami                         = data.aws_ami.ubuntu_2404.id
  instance_type               = "t3.small"
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.ec2_basic.id]
  associate_public_ip_address = true
  key_name                    = aws_key_pair.staging.key_name
  # Root disk ampliado en staging por espacio insuficiente durante builds
  # Docker/Next.js (error ENOSPC en disco de 8 GB).
  root_block_device {
    volume_type = "gp3"
    volume_size = 20
  }

  tags = merge(local.common_tags, {
    Name = "tfm-staging-ec2"
  })
}

# 7) Elastic IP opcional para IP pública estática.
resource "aws_eip" "ec2_public_ip" {
  count  = local.enable_eip ? 1 : 0
  domain = "vpc"

  tags = merge(local.common_tags, {
    Name = "tfm-staging-eip"
  })
}

resource "aws_eip_association" "ec2_public_ip_assoc" {
  count         = local.enable_eip ? 1 : 0
  instance_id   = aws_instance.app.id
  allocation_id = aws_eip.ec2_public_ip[0].id
}
