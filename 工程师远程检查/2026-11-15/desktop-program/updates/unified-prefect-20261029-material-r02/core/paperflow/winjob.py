"""Thin Win32 Job adapter. Creation-time membership; no uncontained fallback."""
from __future__ import annotations

import ctypes as C
from ctypes import wintypes as W
import os
from pathlib import Path
import subprocess
import sys


if os.name != 'nt':
    raise RuntimeError('managed workers require Windows 10 or later')

K = C.WinDLL('kernel32', use_last_error=True)
SIZE = C.c_size_t


def api(name, result, *arguments):
    function = getattr(K, name)
    function.restype, function.argtypes = result, arguments
    return function


class IO(C.Structure):
    _fields_ = [(name, C.c_ulonglong) for name in ('read_ops', 'write_ops', 'other_ops', 'read', 'write', 'other')]


class Limits(C.Structure):
    _fields_ = [('process_time', C.c_longlong), ('job_time', C.c_longlong), ('flags', W.DWORD),
                ('min_working_set', SIZE), ('max_working_set', SIZE), ('active_limit', W.DWORD),
                ('affinity', SIZE), ('priority', W.DWORD), ('scheduling', W.DWORD)]


class ExtendedLimits(C.Structure):
    _fields_ = [('basic', Limits), ('io', IO), ('process_memory', SIZE), ('job_memory', SIZE),
                ('peak_process_memory', SIZE), ('peak_job_memory', SIZE)]


class Accounting(C.Structure):
    _fields_ = [(name, C.c_longlong) for name in ('user', 'kernel', 'period_user', 'period_kernel')] + [
        (name, W.DWORD) for name in ('page_faults', 'total', 'active', 'terminated')]


class Startup(C.Structure):
    _fields_ = [('cb', W.DWORD), ('reserved', W.LPWSTR), ('desktop', W.LPWSTR), ('title', W.LPWSTR),
                ('x', W.DWORD), ('y', W.DWORD), ('width', W.DWORD), ('height', W.DWORD),
                ('x_chars', W.DWORD), ('y_chars', W.DWORD), ('fill', W.DWORD), ('flags', W.DWORD),
                ('show', W.WORD), ('reserved_bytes', W.WORD), ('reserved_ptr', C.c_void_p),
                ('stdin', W.HANDLE), ('stdout', W.HANDLE), ('stderr', W.HANDLE)]


class StartupEx(C.Structure):
    _fields_ = [('startup', Startup), ('attributes', C.c_void_p)]


class ProcessInfo(C.Structure):
    _fields_ = [('process', W.HANDLE), ('thread', W.HANDLE), ('pid', W.DWORD), ('tid', W.DWORD)]


close_handle = api('CloseHandle', W.BOOL, W.HANDLE)
create_job = api('CreateJobObjectW', W.HANDLE, C.c_void_p, W.LPCWSTR)
open_job = api('OpenJobObjectW', W.HANDLE, W.DWORD, W.BOOL, W.LPCWSTR)
set_job = api('SetInformationJobObject', W.BOOL, W.HANDLE, C.c_int, C.c_void_p, W.DWORD)
query_job = api('QueryInformationJobObject', W.BOOL, W.HANDLE, C.c_int, C.c_void_p, W.DWORD, C.c_void_p)
terminate_job = api('TerminateJobObject', W.BOOL, W.HANDLE, W.UINT)
get_handle_info = api('GetHandleInformation', W.BOOL, W.HANDLE, C.POINTER(W.DWORD))
set_handle_info = api('SetHandleInformation', W.BOOL, W.HANDLE, W.DWORD, W.DWORD)
init_attributes = api('InitializeProcThreadAttributeList', W.BOOL, C.c_void_p, W.DWORD, W.DWORD, C.POINTER(SIZE))
update_attribute = api('UpdateProcThreadAttribute', W.BOOL, C.c_void_p, W.DWORD, SIZE, C.c_void_p, SIZE, C.c_void_p, C.c_void_p)
delete_attributes = api('DeleteProcThreadAttributeList', None, C.c_void_p)
create_process = api('CreateProcessW', W.BOOL, W.LPCWSTR, W.LPWSTR, C.c_void_p, C.c_void_p,
                     W.BOOL, W.DWORD, C.c_void_p, W.LPCWSTR, C.c_void_p, C.POINTER(ProcessInfo))
resume_thread = api('ResumeThread', W.DWORD, W.HANDLE)
open_process = api('OpenProcess', W.HANDLE, W.DWORD, W.BOOL, W.DWORD)
process_times = api('GetProcessTimes', W.BOOL, W.HANDLE, C.POINTER(W.FILETIME), C.POINTER(W.FILETIME),
                    C.POINTER(W.FILETIME), C.POINTER(W.FILETIME))
wait_handle = api('WaitForSingleObject', W.DWORD, W.HANDLE, W.DWORD)
get_exit_code = api('GetExitCodeProcess', W.BOOL, W.HANDLE, C.POINTER(W.DWORD))
pipe_client_pid = api('GetNamedPipeClientProcessId', W.BOOL, W.HANDLE, C.POINTER(W.ULONG))


def checked(result):
    if not result:
        raise C.WinError(C.get_last_error())
    return result


def _identity(handle, pid):
    creation, exit_time, kernel, user = (W.FILETIME() for _ in range(4))
    checked(process_times(handle, C.byref(creation), C.byref(exit_time), C.byref(kernel), C.byref(user)))
    return dict(pid=int(pid), created_at=(creation.dwHighDateTime << 32) | creation.dwLowDateTime)


def process_identity(pid):
    handle = checked(open_process(0x1000, False, pid))  # QUERY_LIMITED_INFORMATION
    try:
        return _identity(handle, pid)
    finally:
        close_handle(handle)


def identity_alive(identity):
    # A recycled PID may now belong to a process whose synchronization access
    # is restricted. Creation identity needs only QUERY_LIMITED_INFORMATION;
    # prove a mismatch before asking to wait on that unrelated process.
    handle = open_process(0x1000, False, int(identity['pid']))
    if not handle:
        if C.get_last_error() == 87:
            return False
        raise C.WinError(C.get_last_error())
    try:
        if _identity(handle, identity['pid']) != identity:
            return False
    finally:
        close_handle(handle)
    # Recheck creation identity on the waitable handle as well, so PID reuse
    # between the two opens cannot qualify a different process as the owner.
    handle = open_process(0x1000 | 0x100000, False, int(identity['pid']))
    if not handle:
        if C.get_last_error() == 87:  # no such PID; permission errors are not exit evidence
            return False
        raise C.WinError(C.get_last_error())
    try:
        if _identity(handle, identity['pid']) != identity:
            return False
        status = wait_handle(handle, 0)
        if status == 0:
            return False
        if status == 258:
            return True
        if status == 0xFFFFFFFF:
            raise C.WinError(C.get_last_error())
        raise RuntimeError('process wait status unknown')
    finally:
        close_handle(handle)


def named_pipe_peer(connection):
    pid = W.ULONG()
    checked(pipe_client_pid(connection.fileno(), C.byref(pid)))
    return process_identity(pid.value)


class ManagedProcess:
    def __init__(self, info):
        self.handle, self.thread, self.pid = info.process, info.thread, int(info.pid)
        self.identity = _identity(self.handle, self.pid)

    def resume(self):
        if self.thread is None:
            raise RuntimeError('process already resumed')
        if resume_thread(self.thread) == 0xFFFFFFFF:
            raise C.WinError(C.get_last_error())
        close_handle(self.thread)
        self.thread = None

    def poll(self):
        status = wait_handle(self.handle, 0)
        if status == 258:
            return None
        if status != 0:
            raise C.WinError(C.get_last_error())
        code = W.DWORD()
        checked(get_exit_code(self.handle, C.byref(code)))
        return code.value

    def close(self):
        for name in ('thread', 'handle'):
            handle = getattr(self, name)
            if handle:
                close_handle(handle)
                setattr(self, name, None)


class Job:
    def __init__(self, name):
        if sys.getwindowsversion().major < 10:
            raise RuntimeError('creation-time Job binding requires Windows 10+')
        self.name = name
        self.handle = checked(create_job(None, name))
        if C.get_last_error() == 183:
            self.close()
            raise ValueError('Job name already exists')
        try:
            limits = ExtendedLimits()
            limits.basic.flags = 0x2000  # KILL_ON_JOB_CLOSE; neither breakaway flag
            checked(set_job(self.handle, 9, C.byref(limits), C.sizeof(limits)))
            checked(set_handle_info(self.handle, 1, 0))
        except BaseException:
            self.close()
            raise

    @classmethod
    def open(cls, name):
        handle = open_job(0x0004 | 0x0008, False, name)  # QUERY | TERMINATE
        if not handle:
            if C.get_last_error() == 2:
                return None
            raise C.WinError(C.get_last_error())
        job = cls.__new__(cls)
        job.name, job.handle = name, handle
        return job

    @property
    def inheritable(self):
        flags = W.DWORD()
        checked(get_handle_info(self.handle, C.byref(flags)))
        return bool(flags.value & 1)

    def active_count(self):
        result = Accounting()
        checked(query_job(self.handle, 1, C.byref(result), C.sizeof(result), None))
        return int(result.active)

    def pids(self):
        count = 32
        while count <= 65536:
            buffer = C.create_string_buffer(8 + count * C.sizeof(SIZE))
            if query_job(self.handle, 3, buffer, len(buffer), None):
                actual = W.DWORD.from_buffer(buffer, 4).value
                return sorted((SIZE * actual).from_buffer(buffer, 8))
            if C.get_last_error() != 234:
                raise C.WinError(C.get_last_error())
            count *= 2
        raise RuntimeError('Job process list exceeds verification bound')

    def spawn(self, argv, *, cwd, env=None):
        size = SIZE()
        init_attributes(None, 1, 0, C.byref(size))
        buffer = C.create_string_buffer(size.value)
        checked(init_attributes(buffer, 1, 0, C.byref(size)))
        try:
            handles = (W.HANDLE * 1)(self.handle)
            checked(update_attribute(buffer, 0, 0x0002000D, handles, C.sizeof(handles), None, None))
            startup = StartupEx()
            startup.startup.cb = C.sizeof(startup)
            startup.attributes = C.cast(buffer, C.c_void_p)
            command = C.create_unicode_buffer(subprocess.list2cmdline([str(arg) for arg in argv]))
            environment = os.environ.copy()
            if env:
                environment.update(env)
            block = C.create_unicode_buffer('\0'.join(f'{k}={v}' for k, v in sorted(environment.items())) + '\0\0')
            info = ProcessInfo()
            checked(create_process(str(argv[0]), command, None, None, False,
                                   0x00080000 | 0x00000004 | 0x08000000 | 0x00000400,
                                   block, str(Path(cwd).resolve()), C.byref(startup), C.byref(info)))
            try:
                return ManagedProcess(info)
            except BaseException:
                self.terminate()
                close_handle(info.thread)
                close_handle(info.process)
                raise
        finally:
            delete_attributes(buffer)

    def terminate(self):
        checked(terminate_job(self.handle, 0xE0000001))

    def close(self):
        if self.handle:
            close_handle(self.handle)
            self.handle = None
