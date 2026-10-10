"""迁移旧设置并从分组读取哨兵检测、恢复与补发配置。"""

from copy import deepcopy
import json
from pathlib import Path

SCHEMA = json.loads((Path(__file__).resolve().parents[1] / "_conf_schema.json")
                    .read_text(encoding="utf-8-sig"))
GROUPS = {key: field["items"] for key, field in SCHEMA.items()
          if field["type"] == "object" and not field.get("invisible")}
FIELD_GROUP = {key: group for group, items in GROUPS.items() for key in items}
LAYOUT_VERSION = "settings_layout_version"


def migrate(config: dict) -> bool:
    """首次升级把旧字段复制到分组并保存布局版本；后续调用不再覆盖分组。"""
    if config.get(LAYOUT_VERSION, 0) >= 1:
        return False
    for name, fields in GROUPS.items():
        target = config.setdefault(name, {})
        for key, field in fields.items():
            if key in config:
                target[key] = deepcopy(config[key])
            elif key not in target:
                target[key] = deepcopy(field.get("default"))
    config[LAYOUT_VERSION] = 1
    save = getattr(config, "save_config", None)
    if callable(save):
        save()
    return True


def value(config: dict, key: str, default=None):
    """布局迁移后读取分组值，旧布局读取平铺值；空值使用调用方默认。"""
    group = FIELD_GROUP.get(key)
    if config.get(LAYOUT_VERSION, 0) >= 1 and group is not None:
        result = config.get(group, {}).get(key, default)
    else:
        result = config.get(key, default)
    return default if result is None else result
