# Plugin API Change Validation Plan

## 目的
验证已实现的三项宿主扩展需求：

1. `register_uninstall_hook` / `PluginLoader.unload_plugin` 卸载钩子执行
2. 插件验证器修复：相对 import / `submodule_search_locations`
3. 插件技能注册 API：`register_skill_provider` 是否已对齐现有技能系统

---

## 目标范围

### 1. `register_uninstall_hook` 替换 `PluginLoader.unload_plugin` monkey-patch

验证要点：

- `PluginApi.register_uninstall_hook()` 已经向 `PluginRegistry` 注册卸载钩子
- `PluginLoader.unload_plugin()` 在卸载插件时会执行该钩子
- 卸载钩子支持同步与异步回调
- 卸载钩子错误不会阻塞卸载流程

相关实现：

- `src/qwenpaw/plugins/api.py`
- `src/qwenpaw/plugins/registry.py`
- `src/qwenpaw/plugins/loader.py`

检验测试：

- `tests/unit/plugins/test_plugin_api_extensions.py::TestUninstallHook`
    - `test_register_uninstall_hook_stores_in_registry`
    - `test_uninstall_hooks_sorted_by_priority`
    - `test_uninstall_hooks_cleaned_on_unregister`
    - `test_unload_plugin_calls_uninstall_hooks`
    - `test_uninstall_hook_async_callback`
    - `test_uninstall_hook_error_isolated`

验证命令：

```bash
cd "d:\QP Project\QwenPaw"
python -m pytest tests/unit/plugins/test_plugin_api_extensions.py -q
```

期望结果：

- 所有 `TestUninstallHook` 下测试通过
- 无异常抛出

---

## 2. 插件验证器相对 import / `submodule_search_locations`

验证要点：

- 插件加载/验证时使用 `importlib.util.spec_from_file_location(..., submodule_search_locations=[plugin_dir])`
- 模块 `__package__` / `__path__` 设定正确，支持 `from .constants import ...` 之类的插件内部相对引用

相关实现：

- `src/qwenpaw/plugins/loader.py`

检验测试：

- `tests/unit/plugins/test_plugin_api_extensions.py::TestPluginValidatorImports`
    - `test_validator_sets_submodule_search_locations`
    - `test_validator_without_search_locations_fails`

验证命令：

```bash
cd "d:\QP Project\QwenPaw"
python -m pytest tests/unit/plugins/test_plugin_api_extensions.py -q
```

期望结果：

- `test_validator_sets_submodule_search_locations` 成功
- `test_validator_without_search_locations_fails` 确认缺失时会失败

---

## 3. typed `register_skill_provider` 是否已对齐现有 skill system

验证要点：

- `PluginApi.register_skill_provider()` 已暴露并完成 skill provider 注册契约
- 调用后会注册 startup hook 和 uninstall hook
- plugin skills 安装时会调用宿主技能系统的 manifest reconcile 逻辑
- 卸载时会根据 `source=plugin:{plugin_id}` 清理 workspace manifest

相关实现：

- `src/qwenpaw/plugins/api.py`
- `src/qwenpaw/agents/skill_system/registry.py`
- `src/qwenpaw/agents/skill_system/store.py`

检验测试：

- `tests/unit/plugins/test_plugin_api_extensions.py::TestRegisterSkillProvider`
    - `test_register_skill_provider_registers_hooks`
    - `test_register_skill_provider_default_channels`
    - `test_register_skill_provider_source_tag`

验证命令：

```bash
cd "d:\QP Project\QwenPaw"
python -m pytest tests/unit/plugins/test_plugin_api_extensions.py -q
```

期望结果：

- 所有 `TestRegisterSkillProvider` 下测试通过
- `install_skills_{plugin_id}` startup hook 注册
- `uninstall_skills_{plugin_id}` uninstall hook 注册
- 默认 channels 为 `['all']`

---

## 4. 综合验证建议

### 4.1 运行指定单测文件

```bash
cd "d:\QP Project\QwenPaw"
python -m pytest tests/unit/plugins/test_plugin_api_extensions.py -q
```

### 4.2 如果需要更细粒度验证

- 执行单个测试：
  - `python -m pytest tests/unit/plugins/test_plugin_api_extensions.py -q -k UninstallHook`
  - `python -m pytest tests/unit/plugins/test_plugin_api_extensions.py -q -k PluginValidatorImports`
  - `python -m pytest tests/unit/plugins/test_plugin_api_extensions.py -q -k RegisterSkillProvider`

### 4.3 手工验证补充

1. 验证卸载钩子注册
   - 在插件 backend 代码中调用 `api.register_uninstall_hook(...)`
   - 卸载插件后确认卸载回调执行

2. 验证相对 import
   - 在插件目录下编写 `plugin.py` 和 `constants.py`
   - 运行插件加载/验证路径，确认 `from .constants import ...` 不报错

3. 验证技能注册
   - 在插件 backend 中调用 `api.register_skill_provider(skills_dir=...)`
   - 启动宿主后确认 workspace skill manifest 中 `source` 为 `plugin:{plugin_id}`，`enabled` / `channels` 按预期写入

---

## 5. 记录与结果

请将验证结果记录在本文件末尾，说明：

- 是否通过所有相关单元测试
- 是否存在额外补充验证需求
- 任何观察到的实现差异或代码行为

---

## 6. 验证结果

### 6.1 单测执行结果

- 执行命令：
  ```bash
  cd "d:\QP Project\QwenPaw"
  python -m pytest tests/unit/plugins/test_plugin_api_extensions.py -q
  ```
- 结果：`11 passed`
- 说明：目标验证文件中所有测试用例均通过，覆盖了 `register_uninstall_hook`、插件验证器相对 import、以及 `register_skill_provider` 注册契约。

### 6.2 结论

- `register_uninstall_hook` 相关实现已通过验证
- 插件验证器相对 import / `submodule_search_locations` 修复已通过验证
- `register_skill_provider` API 与当前技能系统注册逻辑对齐已通过验证

### 6.3 备注

- 当前验证依赖已有单元测试覆盖。如需进一步确认插件运行时行为，可补充集成测试或手工验证插件加载/卸载流程。
