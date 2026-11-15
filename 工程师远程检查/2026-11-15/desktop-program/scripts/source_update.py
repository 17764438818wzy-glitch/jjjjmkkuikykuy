"""Verify an immutable source overlay at a safe normal launch, without writes."""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

_PROFILE_RUNTIME_ROOT = 'desktop/profile_runtime/python/'
_PROFILE_RUNTIME_FILES = frozenset({
    'json_repair/__init__.py', 'json_repair/__main__.py',
    'json_repair/json_parser.py', 'json_repair/json_repair.py',
    'json_repair/parse_array.py', 'json_repair/parse_comment.py',
    'json_repair/parse_number.py', 'json_repair/parse_object.py',
    'json_repair/parse_string.py', 'json_repair/parser_parenthesized.py',
    'json_repair/parser_schema.py', 'json_repair/schema_repair.py',
    'json_repair/parse_string_helpers/__init__.py',
    'json_repair/parse_string_helpers/object_value_context.py',
    'json_repair/parse_string_helpers/parse_boolean_or_null.py',
    'json_repair/parse_string_helpers/parse_json_llm_block.py',
    'json_repair/utils/__init__.py', 'json_repair/utils/constants.py',
    'json_repair/utils/json_context.py', 'json_repair/utils/object_comparer.py',
    'json_repair/utils/pattern_properties.py',
    'json_repair/utils/string_file_wrapper.py', 'licenses/LICENSE',
})

def _sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def _inside(root,relative):
    if not isinstance(relative,str) or not relative or Path(relative).is_absolute():
        raise ValueError('源码更新路径无效')
    path=(root/relative).resolve()
    if not path.is_relative_to(root.resolve()):raise ValueError('源码更新路径越界')
    return path

def activate_source_update(root):
    """Caller must first protect all existing owners; never replace loaded code."""
    root=Path(root).resolve();selection=root/'config/source-update.json'
    if not selection.exists():
        _guard_legacy_prepared(root)
        return None
    row=json.loads(selection.read_text(encoding='utf8'))
    if row.get('schema')!='source-update-selection/1':raise ValueError('源码更新选择记录无效')
    update=_inside(root,row.get('directory'))
    if not update.is_relative_to(root/'updates'):raise ValueError('源码更新必须位于统一目录 updates 内')
    path=update/'manifest.json'
    if _sha(path)!=row.get('manifest_sha256'):raise ValueError('源码更新清单发生变化，未加载新版')
    manifest=json.loads(path.read_text(encoding='utf8'))
    if manifest.get('schema')=='source-update/2':return _activate_paired(root,update,manifest)
    _guard_legacy_prepared(root)
    if manifest.get('schema')!='source-update/1':raise ValueError('源码更新清单版本不兼容')
    desktop=update/'desktop';files=manifest.get('files',[]);verified=set();modules=set()
    if not files or not manifest.get('live_baseline'):raise ValueError('源码更新缺少版本绑定')
    for record in files:
        source=_inside(update,record.get('file'))
        if source.parent!=desktop or source.suffix!='.py' or source.name=='project_paths.py' or source in verified:
            raise ValueError('源码更新模块位置或编号无效')
        if _sha(source)!=record.get('sha256'):raise ValueError('源码更新文件发生变化，未加载新版')
        verified.add(source);modules.add(source.stem)
    if verified!={p.resolve() for p in desktop.glob('*.py')}:raise ValueError('源码更新存在未登记模块')
    for record in manifest['live_baseline']:
        source=_inside(root,record.get('file'))
        if not source.is_relative_to(root/'desktop') or _sha(source)!=record.get('sha256'):
            raise ValueError('基础源码版本已变化，未混用新旧模块')
    if any(name in sys.modules for name in modules):
        raise RuntimeError('源码模块已载入；新版仅能在程序完全退出后由统一入口加载')
    sys.path.insert(0,str(desktop))
    return desktop


def _guard_legacy_prepared(root):
    paths=json.loads((root/'config/paths.json').read_text(encoding='utf8'))
    data=Path(os.environ.get('PAPER_UPSTREAM_DATA_DIR',paths['data_dir']))
    if (data/'prepared-current-scope.json').exists():
        raise RuntimeError('已存在持续范围控制记录；旧版不能恢复派发。请使用配套版本保留停止和回执后核对。')


def _reject_links(root,path):
    current=path
    while current!=root:
        if current.is_symlink() or current.is_junction():
            raise ValueError('成对更新不能包含链接')
        current=current.parent


def _verify_profile_runtime(update,manifest):
    relative=manifest.get('profile_json_runtime')
    if relative is None:return set()
    expected=_PROFILE_RUNTIME_ROOT+'runtime-manifest.json'
    if relative!=expected:raise ValueError('JSON组件清单位置无效')
    index=_inside(update,relative)
    component=json.loads(index.read_text(encoding='utf8'))
    if (component.get('schema')!='profile-json-runtime/1'
            or component.get('package')!='json_repair'
            or component.get('license')!='licenses/LICENSE'):
        raise ValueError('JSON组件身份无效')
    declared={}
    for record in component.get('files',[]):
        name=record.get('file')
        if name not in _PROFILE_RUNTIME_FILES or name in declared:
            raise ValueError('JSON组件文件未登记或重复')
        declared[name]=record.get('sha256')
    if set(declared)!=_PROFILE_RUNTIME_FILES:raise ValueError('JSON组件或许可证缺失')
    release={row['file']:row['sha256'] for row in manifest['files']}
    if expected not in release or 'desktop/profile_json.py' not in release:
        raise ValueError('JSON加载模块或组件清单未纳入发布')
    for name,digest in declared.items():
        if release.get(_PROFILE_RUNTIME_ROOT+name)!=digest:
            raise ValueError('JSON组件与发布哈希闭包不一致')
    return {expected,*(_PROFILE_RUNTIME_ROOT+name for name in declared)}


def _activate_paired(root,update,manifest):
    """Select the entire native controller package and desktop as one release."""
    desktop=update/'desktop';core=update/'core';verified=set();modules=set()
    if not manifest.get('files') or not manifest.get('live_baseline'):raise ValueError('源码更新缺少完整版本绑定')
    runtime_files=_verify_profile_runtime(update,manifest)
    for record in manifest['files']:
        raw=update/record.get('file','')
        source=_inside(update,record.get('file'))
        _reject_links(update,raw)
        desktop_module=source.parent==desktop and source.suffix=='.py' and source.name!='project_paths.py'
        core_module=source.is_relative_to(core/'paperflow') and (source.suffix=='.py' or source==core/'paperflow/native_group.mjs' or
            (source.parent==core/'paperflow/prompts' and source.suffix in {'.txt','.json','.yml'}))
        runtime_file=record['file'] in runtime_files
        if not (desktop_module or core_module or runtime_file) or source in verified:raise ValueError('成对更新模块位置无效')
        if _sha(source)!=record.get('sha256'):raise ValueError('成对更新模块哈希变化')
        verified.add(source)
        if desktop_module:modules.add(source.stem)
    actual={p.resolve() for p in desktop.glob('*.py')}|{p.resolve() for p in (core/'paperflow').rglob('*') if p.is_file() and '__pycache__' not in p.parts}
    if runtime_files:
        for path in (desktop/'profile_runtime').rglob('*'):
            _reject_links(update,path)
            if path.is_file() and '__pycache__' not in path.parts:actual.add(path.resolve())
        if any(name=='json_repair' or name.startswith('json_repair.') for name in sys.modules):
            raise RuntimeError('JSON组件已载入；配套发布只能在原执行者退出后加载')
    if actual!=verified or not (core/'paperflow/__init__.py').is_file():raise ValueError('成对更新存在未登记或缺少的模块')
    for record in manifest['live_baseline']:
        source=_inside(root,record.get('file'))
        if _sha(source)!=record.get('sha256'):raise ValueError('基础源码变化，不能混用版本')
    if any(name in sys.modules for name in modules) or any(name=='paperflow' or name.startswith('paperflow.') for name in sys.modules):
        raise RuntimeError('控制模块已载入；只能在原执行者退出后选择版本')
    os.environ['PAPERFLOW_SELECTED_CORE']=str(core)
    os.environ['PAPERFLOW_BASE_ROOT']=str(root)
    sys.path[:0]=[str(desktop),str(core)]
    return desktop
