FROM python:3.12-slim

WORKDIR /app

# 安装系统依赖（使用国内镜像）
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update --fix-missing && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    curl \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖（使用国内镜像加速）
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 复制项目文件
COPY . .

# 创建输出目录
RUN mkdir -p /app/output

# 设置环境变量
ENV PYTHONUNBUFFERED=1
# 设置时区为北京时间
ENV TZ=Asia/Shanghai
# ENV DEMO_MODE=true

# 运行主程序
CMD ["python", "main.py"]