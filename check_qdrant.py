import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

from qdrant_client import QdrantClient
c = QdrantClient(host='localhost', port=6333, https=False, prefer_grpc=False, check_compatibility=False)
cols = c.get_collections().collections
print(f'{len(cols)} collections:')
for col in cols:
    print(f'  - {col.name}')

