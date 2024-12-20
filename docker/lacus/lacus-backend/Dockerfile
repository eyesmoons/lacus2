# 使用OpenJDK 8
FROM openjdk:8

# 安装必要工具
RUN apt-get update && apt-get install -y bash procps

# 设置工作目录
WORKDIR /app

# 复制tar包
COPY lacus-dist-1.0.0-all.tar.gz /app/

# 解压并设置权限
RUN tar -xzf lacus-dist-1.0.0-all.tar.gz && \
    cp -r lacus-dist-1.0.0/* . && \
    rm -rf lacus-dist-1.0.0 && \
    rm lacus-dist-1.0.0-all.tar.gz && \
    chmod 755 bin/start_docker.sh && \
    chown root:root bin/start_docker.sh && \
    chmod 755 bin && \
    sed -i '1c #!/bin/bash' bin/start_docker.sh && \
    sed -i 's/\r$//' bin/start_docker.sh && \
    mkdir -p logs

# 暴露应用端口
EXPOSE 19888

# 使用root用户运行
USER root

# 创建启动脚本
RUN echo '#!/bin/bash\n\
cd /app\n\
\n\
AppName=lacus-admin-1.0.0.jar\n\
\n\
# JVM参数\n\
JVM_OPTS="-Dname=$AppName -Duser.timezone=Asia/Shanghai -Xms2g -Xmx2g -XX:MetaspaceSize=512m -XX:MaxMetaspaceSize=512m -XX:+HeapDumpOnOutOfMemoryError -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:NewRatio=1 -XX:SurvivorRatio=30 -XX:+UseParallelGC -XX:+UseParallelOldGC"\n\
\n\
# 直接在前台运行 Java 应用\n\
exec java $JVM_OPTS -jar boot/$AppName' > /app/docker-entrypoint.sh && \
chmod +x /app/docker-entrypoint.sh

# 使用新的启动脚本
ENTRYPOINT ["/app/docker-entrypoint.sh"]