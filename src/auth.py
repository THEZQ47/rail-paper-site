from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


class SourceAuthProvider:
    """统一从环境变量读取各数据源鉴权头。"""

    def __init__(
        self,
        source_configs: Mapping[str, Any],
        env: Mapping[str, str] | None = None,
    ) -> None:
        self._source_configs = source_configs
        self._env = env if env is not None else os.environ

    def get_headers(self, source_name: str) -> dict[str, str]:
        source_cfg = self._source_configs.get(source_name, {})
        auth_cfg = source_cfg.get("auth", {})
        header_from_env = auth_cfg.get("header_from_env", {})

        headers: dict[str, str] = {}
        for header_name, env_key in header_from_env.items():
            env_name = str(env_key)
            value = self._env.get(env_name)
            if value:
                headers[str(header_name)] = value
        return headers

    def missing_env_keys(self, source_name: str) -> list[str]:
        source_cfg = self._source_configs.get(source_name, {})
        auth_cfg = source_cfg.get("auth", {})
        header_from_env = auth_cfg.get("header_from_env", {})

        missing: list[str] = []
        for env_key in header_from_env.values():
            env_name = str(env_key)
            if not self._env.get(env_name):
                missing.append(env_name)
        return missing

