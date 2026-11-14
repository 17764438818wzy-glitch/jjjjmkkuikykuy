"""Fixed Linux host observation over existing SSH, separate from business readers.

Uses the established Paramiko/known_hosts login pattern. The fixed OPS collector
can opt into sudo via stdin; no deployment, service changes or task APIs.
Credentials stay in memory; only local observation caches are written.
"""
import json
from pathlib import Path
import shlex
import time

PROBE = r'''
import json,os,time,subprocess,shutil
from pathlib import Path
def cpu():
    row=list(map(int,Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
    return sum(row),row[3]+row[4]
a=cpu();time.sleep(.1);b=cpu();total=b[0]-a[0]
mem={p[0].rstrip(':'):int(p[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines() if len(p:=line.split())>=2}
network={}
for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
    name,body=line.split(':');name=name.strip();parts=body.split()
    if name=='lo' or name.startswith(('veth','docker','br-','virbr')):continue
    network[name]={'rx_bytes':int(parts[0]),'tx_bytes':int(parts[8])}
io={}
for line in Path('/proc/diskstats').read_text().splitlines():
    p=line.split();name=p[2]
    if name.startswith(('loop','ram','dm-')) or Path('/sys/class/block',name,'partition').exists():continue
    io[name]={'read_bytes':int(p[5])*512,'write_bytes':int(p[9])*512,'busy_ms':int(p[12])}
disks=[];seen=set()
for line in Path('/proc/self/mounts').read_text().splitlines():
    dev,mount,kind=line.split()[:3];mount=mount.replace('\\040',' ').replace('\\134','\\')
    if not dev.startswith('/dev/') or dev.startswith('/dev/loop') or kind in ('squashfs','iso9660') or dev in seen:continue
    try:
        s=os.statvfs(mount);disks.append({'device':dev,'mount':mount,'filesystem':kind,'total':s.f_blocks*s.f_frsize,'free':s.f_bavail*s.f_frsize});seen.add(dev)
    except OSError:pass
pressure={}
for kind in ('cpu','memory','io'):
    try:pressure[kind]=Path('/proc/pressure',kind).read_text().strip()
    except OSError:pressure[kind]=None
gpus=[];gpu_status='unavailable'
if shutil.which('nvidia-smi'):
    try:
        r=subprocess.run(['nvidia-smi','--query-gpu=name,memory.total,memory.used,utilization.gpu','--format=csv,noheader,nounits'],capture_output=True,text=True,timeout=3)
        if r.returncode==0:
            for line in r.stdout.splitlines():
                name,capacity,used,util=[x.strip() for x in line.rsplit(',',3)]
                gpus.append({'name':name,'total':int(capacity)*1024**2,'used':int(used)*1024**2,'utilization':float(util)})
            gpu_status='ok'
    except (OSError,ValueError,subprocess.TimeoutExpired):pass
print(json.dumps({'hostname':os.uname().nodename,'sample_time':time.time(),'cpu_percent':round(100*(1-(b[1]-a[1])/total),1) if total>0 else None,
    'cpu_sample_seconds':.1,'cpu_count':os.cpu_count(),'load_average':list(os.getloadavg()),
    'memory_total':mem.get('MemTotal'),'memory_available':mem.get('MemAvailable'),
    'swap_total':mem.get('SwapTotal'),'swap_free':mem.get('SwapFree'),'disks':disks[:16],
    'network':network,'disk_io':io,'pressure':pressure,'gpus':gpus,'gpu_status':gpu_status}))
'''


def rates(current,previous):
    elapsed=current.get('sample_time',0)-(previous or {}).get('sample_time',0)
    result={}
    for category,fields in [('network',('rx_bytes','tx_bytes')),('disk_io',('read_bytes','write_bytes','busy_ms'))]:
        rows=[]
        for name,counters in current.get(category,{}).items():
            old=(previous or {}).get(category,{}).get(name,{})
            row={'name':name}
            for field in fields:
                delta=counters.get(field,0)-old.get(field,0)
                row[field+'_per_second']=round(delta/elapsed,2) if previous and field in old and delta>=0 and 0<elapsed<60 else None
            rows.append(row)
        result[category]=rows
    return result


def connect(host,username,password=None,key_file=None):
    import paramiko
    client=paramiko.SSHClient();client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(host,username=username,password=password,key_filename=key_file,
        look_for_keys=False,allow_agent=False,timeout=5,auth_timeout=5,banner_timeout=5)
    client.get_transport().set_keepalive(10)
    return client


def observe(client):
    stdin,stdout,stderr=client.exec_command('python3 -c '+shlex.quote(PROBE),timeout=5)
    stdin.close()
    data=stdout.read(65537)
    if len(data)>65536:raise ValueError('Resource sample too large')
    if stdout.channel.recv_exit_status()!=0:raise ValueError('Resource sample unavailable')
    value=json.loads(data)
    if not isinstance(value,dict) or not isinstance(value.get('memory_total'),int):raise ValueError('Invalid host sample')
    return value


def publish_native_runtime(client, configuration, *, password=None, timeout=4.):
    """Attach the OPS collector to this existing observer, not a second daemon.

    The trusted local sidecar pins the already deployed OPS script, full CID,
    service/pipeline and output path. No browser input or server mutation.
    """
    configuration = Path(configuration)
    if not configuration.is_file():
        return
    settings = json.loads(configuration.read_text(encoding='utf8'))
    if settings.get('schema') != 'rag-runtime-monitor/1' or settings.get('enabled') is not True:
        return
    output = Path(settings['output_path']).resolve()
    expected = Path(settings['data_root']).resolve() / 'private/preextract_delivery/native-runtime-observation.json'
    if output != expected:
        raise ValueError('Runtime observer output must use the bound auxiliary cache')
    binding = {key: settings[key] for key in
        ('container', 'container_id', 'service', 'pipeline_id', 'stream', 'consumer_group')}
    previous = None
    started = time.monotonic()
    deadline = started + timeout
    channel = None
    privilege_mode = settings.get('privilege_mode', 'user')

    def remaining():
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise TimeoutError('OPS runtime collection budget exhausted')
        return budget

    def send_all(payload):
        offset = 0
        while offset < len(payload):
            channel.settimeout(remaining())
            sent = channel.send(payload[offset:offset + 32768])
            if sent <= 0:
                raise OSError('OPS runtime input channel closed')
            offset += sent
    try:
        if output.stat().st_size <= 1024**2:
            previous = json.loads(output.read_text(encoding='utf8'))
    except (OSError, ValueError):
        pass
    try:
        if client is None:
            raise OSError('Observer connection unavailable')
        if privilege_mode not in ('user', 'sudo_stdin'):
            raise ValueError('Unsupported OPS observer privilege mode')
        if privilege_mode == 'sudo_stdin':
            if (not isinstance(password, str) or not password
                    or any(char in password for char in '\x00\r\n')
                    or len(password.encode('utf8')) > 4096):
                raise ValueError('OPS sudo stdin credential unavailable')
        script = settings['observer_script']
        if (not isinstance(script, str) or not script.startswith('/')
                or any(char in script for char in '\x00\r\n')):
            raise ValueError('OPS collector requires an absolute pinned path')
        bootstrap = 'import importlib.util,json,sys\n'
        if privilege_mode == 'sudo_stdin':
            # A cached/NOPASSWD sudo may leave the credential line unread. The
            # fixed bootstrap consumes at most that one line, never passing it
            # to collect(). JSON is sent only after this privileged child starts.
            bootstrap += ('sys.stdout.write("paperflow-ops-ready/1\\n");sys.stdout.flush()\n'
                'line=sys.stdin.buffer.readline(4098)\n'
                'if line!=b"paperflow-ops-json/1\\n": line=sys.stdin.buffer.readline(4098)\n'
                'if line!=b"paperflow-ops-json/1\\n": raise ValueError("OPS runtime input framing unavailable")\n')
        bootstrap += (
            'spec=importlib.util.spec_from_file_location("paperflow_ops_runtime",sys.argv[1]);'
            'module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);'
            'print(json.dumps(module.collect(**json.load(sys.stdin))))')
        command = 'python3 -B -c ' + shlex.quote(bootstrap) + ' ' + shlex.quote(script)
        if privilege_mode == 'sudo_stdin':
            command = "sudo -S -p '' -- " + command
        channel = client.get_transport().open_session(timeout=remaining())
        channel.settimeout(remaining())
        channel.exec_command(command)
        payload = json.dumps(dict(binding, previous=previous)).encode('utf8')
        if privilege_mode == 'sudo_stdin':
            send_all(password.encode('utf8') + b'\n')
            ready, expected_ready = bytearray(), b'paperflow-ops-ready/1\n'
            while bytes(ready) != expected_ready:
                channel.settimeout(remaining())
                if channel.recv_ready():
                    ready.extend(channel.recv(256))
                    if len(ready) > len(expected_ready) or not expected_ready.startswith(bytes(ready)):
                        raise ValueError('OPS privileged observer handshake unavailable')
                    continue
                if channel.recv_stderr_ready():
                    channel.recv_stderr(32768)
                    continue
                if channel.exit_status_ready() or channel.closed:
                    raise ValueError('OPS privileged observer unavailable')
                time.sleep(min(.01, remaining()))
            payload = b'paperflow-ops-json/1\n' + payload
        send_all(payload)
        remaining()
        channel.shutdown_write()
        data = bytearray()
        while True:
            channel.settimeout(remaining())
            if channel.recv_ready():
                data.extend(channel.recv(32768))
                if len(data) > 1024**2:
                    raise ValueError('OPS runtime observation too large')
                continue
            if channel.recv_stderr_ready():
                channel.recv_stderr(32768)
                continue
            if channel.exit_status_ready():
                # No recv_exit_status wait without a ready exit event. All
                # sending, draining and exit checks share the same deadline.
                if channel.recv_exit_status() != 0:
                    raise ValueError('OPS runtime observation unavailable')
                break
            if channel.closed:
                raise OSError('OPS runtime channel closed without exit status')
            time.sleep(min(.01, remaining()))
        remaining()
        if len(data) > 1024**2:
            raise ValueError('OPS runtime observation unavailable')
        row = json.loads(data)
        if (row.get('schema') != 'rag-native-runtime-observation/1'
                or row.get('service', '').rstrip('/') != binding['service'].rstrip('/')
                or row.get('pipeline_id') != binding['pipeline_id']
                or row.get('identity', {}).get('container_id') != binding['container_id']):
            raise ValueError('OPS runtime observation identity changed')
    except Exception as exc:
        row = dict(schema='rag-native-runtime-observation/1', service=binding['service'],
            pipeline_id=binding['pipeline_id'], observed_at=None, evaluated_at=time.time(), workers=[],
            startup_enabled=None, collection_complete=False,
            collection_errors=['runtime_observer_unavailable'], error_type=type(exc).__name__)
    finally:
        if channel is not None:
            channel.close()
    row['local_collection_seconds'] = round(time.monotonic() - started, 3)
    row['local_collection_budget_seconds'] = timeout
    row['observer_privilege_mode'] = privilege_mode if privilege_mode in ('user', 'sudo_stdin') else None
    # Only the explicit auxiliary observer cache is written. Original worker
    # timestamps are retained; a failed collection cannot refresh old ready.
    temporary = output.with_name(output.name + '.observer.tmp')
    temporary.write_text(json.dumps(row, ensure_ascii=False), encoding='utf8')
    temporary.replace(output)


def monitor(output,host,username,password=None,key_file=None,once=False):
    output=Path(output);client=None;previous=None
    try:
        while True:
            started=time.monotonic()
            try:
                if client is None or not client.get_transport().is_active():client=connect(host,username,password,key_file)
                current=observe(client)
                result=current|rates(current,previous)|{'host':host,'status':'ok','observed_at':time.time(),
                    'transport':'SSH / fixed read-only probe','sample_seconds':round(time.monotonic()-started,3)}
                previous=current
            except Exception as exc:
                result={'host':host,'status':'unavailable','observed_at':time.time(),'error_kind':type(exc).__name__}
                if client:client.close()
                client=None;previous=None
            try:
                temp=output.with_suffix('.tmp');temp.write_text(json.dumps(result,ensure_ascii=False),encoding='utf8');temp.replace(output)
            except OSError:
                if once:raise
                # The API expires the previous sample; retry after local storage recovers.
                try:print('Telemetry output temporarily unavailable',flush=True)
                except OSError:pass
            # Ordinary hardware is published first. Optional RAG collection is
            # bounded by the remaining original period; it cannot delay this
            # already completed resource observation or fake its timestamp.
            try:
                remaining = max(.1, min(4., 5. - (time.monotonic() - started)))
                publish_native_runtime(client, output.with_name('rag-runtime-monitor.json'),
                                       password=password, timeout=remaining)
            except (OSError, ValueError, TypeError, KeyError):
                pass
            if once:return result
            time.sleep(max(.1,5-(time.monotonic()-started)))
    finally:
        if client:client.close()
