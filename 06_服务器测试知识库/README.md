# 独立测试知识库（部署材料，尚未部署）

这部分放到原局域网Linux服务器的独立目录 `/opt/paperflow-migration-test`，不要覆盖正式网关。
复用服务器已有的 gateway/Qdrant镜像和现有11436嵌入服务；新建18097端口、独立Qdrant容器、collection及objects目录。
镜像源码副本包含于 gateway-source；没有复制正式.env、向量库、对象、账号或业务数据。

服务器操作者执行前核对18097未被占用，然后在本目录运行：

```bash
python3 prepare_test_env.py
docker compose config --quiet
docker compose up -d
docker compose logs --tail=80 gateway
curl --fail http://127.0.0.1:18097/health
```

首次若Qdrant尚未就绪，网关可能短暂重启，等health成功再配置客户端。
若服务器缺少指定gateway镜像，可在该目录 `docker compose build gateway`；会联网安装冻结依赖。
现有11436必须可从容器内访问；桥接地址变动时修改EMBEDDING_URL，勿改正式嵌入服务配置。

新电脑需要的知识库Key是私有.env里的 PAPERFLOW_RAG_API_KEY，不是正式RAGFlow Key，也不是Qdrant内部Key。
用本机安全终端查看并在新电脑02脚本中输入；不要把值写入交接文档或聊天。
`.env`和`test-data`是在未来部署时生成的，本迁移包目前没有这些内容。
此测试网关与正式库物理隔离；若不部署本方案，替代网关必须同样隔离collection、objects和状态。
后续保留test-data中的验收证据；不要用 `down -v` 或删除测试目录来处理未知请求。
