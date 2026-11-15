"""Local stdio worker. Reuses the deployed lib ABI proven in stage A1.

Never initializes a backend, context, GPU, or inference. No filesystem mutations
or socket operations are permitted, including in any imported Python dependency.
"""
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import resource
import sys
import time

sys.dont_write_bytecode = True


def deny_effects(event, args):
    if event.startswith(('socket.', 'os.mkdir', 'os.remove', 'os.rmdir', 'os.rename', 'os.chmod', 'os.utime')):
        raise RuntimeError('offline tokenizer prohibits side effects')
    if event == 'open':
        _, mode, flags = args
        if any(c in (mode or '') for c in 'wax+') or flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            raise RuntimeError('offline tokenizer prohibits writes')


sys.addaudithook(deny_effects)


class Params(C.Structure):
    _fields_ = [('devices', C.c_void_p), ('tensor_buft_overrides', C.c_void_p),
        ('n_gpu_layers', C.c_int32), ('split_mode', C.c_int), ('load_mode', C.c_int), ('lazy_mode', C.c_int), ('main_gpu', C.c_int32),
        ('tensor_split', C.c_void_p), ('progress_callback', C.c_void_p), ('progress_callback_user_data', C.c_void_p), ('kv_overrides', C.c_void_p),
        ('vocab_only', C.c_bool), ('check_tensors', C.c_bool), ('use_extra_bufts', C.c_bool), ('no_host', C.c_bool), ('no_alloc', C.c_bool), ('load_mtp', C.c_bool)]


def main():
    started = time.perf_counter()
    model_path, lib_path = sys.argv[1:3]
    encoder = None
    if len(sys.argv) == 5:
        cache, runtime = sys.argv[3:5]
        sys.path.insert(0, runtime)
        os.environ['TIKTOKEN_CACHE_DIR'] = cache
        import tiktoken
        if tiktoken.__version__ != '0.12.0':
            raise ValueError('unverified unified tokenizer version')
        encoder = tiktoken.get_encoding('cl100k_base')
    ll = C.CDLL(lib_path)
    ll.llama_model_default_params.restype = Params
    ll.llama_model_load_from_file.argtypes = [C.c_char_p, Params]
    ll.llama_model_load_from_file.restype = C.c_void_p
    ll.llama_model_get_vocab.argtypes = [C.c_void_p]
    ll.llama_model_get_vocab.restype = C.c_void_p
    ll.llama_tokenize.argtypes = [C.c_void_p, C.c_char_p, C.c_int32, C.POINTER(C.c_int32), C.c_int32, C.c_bool, C.c_bool]
    ll.llama_tokenize.restype = C.c_int32
    ll.llama_model_free.argtypes = [C.c_void_p]
    params = ll.llama_model_default_params()
    params.vocab_only, params.n_gpu_layers = True, 0
    model = ll.llama_model_load_from_file(model_path.encode(), params)
    if not model:
        raise ValueError('vocabulary-only model load failed')
    vocab = ll.llama_model_get_vocab(model)
    try:
        proc_stat = Path('/proc/self/stat').read_text()
        start_ticks = int(proc_stat[proc_stat.rfind(')')+2:].split()[19])
        print(json.dumps(dict(ready=True, vocab_only=True, context_created=False, inference_requests=0,
            unified=encoder is not None, pid=os.getpid(), tokenizer_loads=1,
            helper_start_ticks=start_ticks, helper_pid_namespace=os.stat('/proc/self/ns/pid').st_ino,
            helper_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            worker_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            load_seconds=time.perf_counter() - started, rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)), flush=True)
        for line in sys.stdin:
            texts = json.loads(line)['texts']
            counts = []
            for text in texts:
                raw = text.encode()
                count = ll.llama_tokenize(vocab, raw, len(raw), None, 0, True, True)
                if count == -2147483648:
                    raise ValueError('tokenizer overflow')
                counts.append(abs(count))
            result = dict(counts=counts, hashes=[hashlib.sha256(t.encode()).hexdigest() for t in texts])
            if encoder is not None:
                result['cl100k_counts'] = [len(encoder.encode(t, disallowed_special=())) for t in texts]
            print(json.dumps(result), flush=True)
    finally:
        ll.llama_model_free(model)


if __name__ == '__main__':
    main()
