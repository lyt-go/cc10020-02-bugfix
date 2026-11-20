"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline

# pipeline 含固定的 GET 路径，必须先于 resource 的 ``/api/{module}/{entry_id}``
# 动态路径注册，否则 ``/api/pipeline/<action>`` 会被当作模块条目解析。
ROUTERS = [pipeline, resource]
