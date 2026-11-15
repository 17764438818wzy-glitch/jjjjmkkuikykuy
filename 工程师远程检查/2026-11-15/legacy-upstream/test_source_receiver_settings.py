"""Normal GUI saves must preserve trusted receiver argv and native parser maps."""
import tkinter as tk

import pytest

from app import Workbench
from auto_config import load_config, save_config
from auto_ui import save_form


@pytest.mark.parametrize('action', ['save', 'close'])
@pytest.mark.parametrize('updated_while_open', [False, True])
def test_gui_preserves_typed_host_settings(tmp_path, action, updated_while_open):
    config = load_config(tmp_path)
    config.update(source_receiver_command=['C:/Python314/python.exe', 'C:/trusted/receiver.py'],
                  dataset_native_parser_map={'approved-paper-dataset': 'naive'},
                  work_pool_auto_start=False)
    save_config(tmp_path, config)
    root = tk.Tk()
    root.withdraw()
    closed = False
    try:
        app = Workbench(root, tmp_path)
        app.ai_vars['text_model'].set('model-edited-by-user')
        if updated_while_open:
            config = load_config(tmp_path)
            config['source_receiver_command'] += ['--config', 'C:/trusted/receiver-config.json']
            config['dataset_native_parser_map']['another-approved-dataset'] = 'naive'
            save_config(tmp_path, config)
        if action == 'close':
            app.close()
            closed = True
        else:
            save_form(app)
        after = load_config(tmp_path)
        assert after['source_receiver_command'] == config['source_receiver_command']
        assert after['dataset_native_parser_map'] == config['dataset_native_parser_map']
        assert after['work_pool_auto_start'] is False
        assert after['text_model'] == 'model-edited-by-user'
    finally:
        if not closed:
            for callback in root.tk.call('after', 'info'):
                root.after_cancel(callback)
            root.destroy()
