#!/usr/bin/env python3
"""Real PG 18.4 two-phase cache checks on an owned disposable backend.

Run with the fleet lock and 24GiB/no-swap scope. No existing database is used.
Usage: cache_prepared_commit.py PROXY_BINARY --output NEW_DIRECTORY
A baseline may return exit 1 with stale-read assertions; setup failures are errors.
"""
import argparse,contextlib,hashlib,json,os,pathlib,secrets,socket,struct,subprocess,time

def port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def frame(tag,payload=b''):return tag+struct.pack('!I',len(payload)+4)+payload
def exact(s,n):
    out=b''
    while len(out)<n:
        b=s.recv(n-len(out))
        if not b:raise EOFError('connection closed')
        out+=b
    return out
def receive(s):
    tag=exact(s,1);n=struct.unpack('!I',exact(s,4))[0]
    if not 4<=n<=1048576:raise ValueError('invalid frame length')
    return tag,exact(s,n-4)
def drain(s,stop=b'Z'):
    out=[]
    for _ in range(100):
        m=receive(s);out.append(m)
        if m[0]==stop:return out
        if stop!=b'Z' and m[0]==b'E':raise RuntimeError(repr(m))
    raise RuntimeError('frame limit')
def connect(p):
    s=socket.create_connection(('127.0.0.1',p),timeout=5)
    try:
        data=struct.pack('!I',196608)+b'user\0audit\0database\0audit\0application_name\0cache_2pc_test\0\0'
        s.sendall(struct.pack('!I',len(data)+4)+data)
        for t,b in drain(s):
            if t==b'E':raise RuntimeError(repr(b))
            if t==b'R' and b!=b'\0\0\0\0':raise RuntimeError('owned fixture must use trust')
        return s
    except BaseException:s.close();raise

def query(s,sql,mode='simple',expect_error=False,before_sync=None):
    if mode=='simple':wire=frame(b'Q',sql.encode()+b'\0')
    else:
        wire=frame(b'P',b'\0'+sql.encode()+b'\0\0\0')+frame(b'B',b'\0\0'+b'\0'*6)+frame(b'E',b'\0'+struct.pack('!I',0))+frame(b'H' if mode=='extended_flush' else b'S')
    s.sendall(wire)
    if mode=='extended_flush':
        response=drain(s,b'C')
        assert any(t==b'C' and b==b'COMMIT PREPARED\0' for t,b in response),response
        # Check another session while this cycle still has no Sync/RFQ.
        try:
            if before_sync is not None:before_sync()
        finally:
            s.sendall(frame(b'S'));response+=drain(s)
    else:response=drain(s)
    errors=[b for t,b in response if t==b'E']
    if bool(errors)!=expect_error:raise AssertionError((sql,errors,expect_error))
    assert response[-1][0]==b'Z'
    return [b[6:].decode() for t,b in response if t==b'D']

def binary_digest(binary):return hashlib.sha256(binary.read_bytes()).hexdigest()

def owns_listener(pid,p):
    """Match a loopback TCP listener inode to the exact owned proxy process."""
    inodes=set()
    for fd in pathlib.Path(f'/proc/{pid}/fd').iterdir():
        try:link=os.readlink(fd)
        except FileNotFoundError:continue
        if link.startswith('socket:['):inodes.add(link[8:-1])
    address=f'0100007F:{p:04X}'
    return any(fields[1]==address and fields[3]=='0A' and fields[9] in inodes
               for fields in (line.split() for line in pathlib.Path('/proc/net/tcp').read_text().splitlines()[1:]))

def backend_identity(owned,name,p):
    inspected=subprocess.run(['docker','inspect',owned],capture_output=True,text=True,timeout=10)
    if inspected.returncode:raise RuntimeError('owned backend no longer exists')
    info=json.loads(inspected.stdout)[0];state=info['State']
    if not state['Running'] or info['Config']['Labels'].get('proxy-cache-2pc-owned')!=name:
        raise RuntimeError('owned backend is not running with its exact label')
    with connect(p) as s:nonce=query(s,'SHOW cluster_name')
    if nonce!=[name]:raise RuntimeError('endpoint is not the owned backend; no test writes permitted')
    return {'id':info['Id'],'pid':state['Pid'],'started_at':state['StartedAt'],'cluster_name':nonce[0]}

def stop_proxy(proxy):
    record={'pid':proxy.pid,'reaped':False,'errors':[]}
    try:
        if proxy.poll() is None:proxy.terminate()
        proxy.wait(timeout=10)
    except Exception as e:
        record['errors'].append(repr(e))
        try:
            if proxy.poll() is None:proxy.kill()
            proxy.wait(timeout=5)
        except Exception as e:record['errors'].append(repr(e))
    try:record['reaped']=proxy.poll() is not None
    except Exception as e:record['errors'].append(repr(e))
    return record

def stop_container(owned,name):
    record={'id':owned,'removed':False,'errors':[]}
    # Recover our exact labeled container even if docker run timed out after
    # creating it. Never stop a name lookup that lacks the ownership label.
    try:
        inspected=subprocess.run(['docker','inspect',owned or name],capture_output=True,text=True,timeout=10)
        if inspected.returncode:
            if 'no such' in inspected.stderr.lower():
                record['removed']=True
                return record
            raise RuntimeError(('docker inspect failed',inspected.stderr))
        info=json.loads(inspected.stdout)[0]
        if info['Config']['Labels'].get('proxy-cache-2pc-owned')!=name:
            raise RuntimeError('container ownership label mismatch')
        owned=info['Id'];record['id']=owned
    except Exception as e:
        record['errors'].append(repr(e));return record
    try:
        stopped=subprocess.run(['docker','stop','--time','5',owned],capture_output=True,text=True,timeout=30)
        record['stop_exit']=stopped.returncode
        if stopped.returncode:record['errors'].append(('docker stop',stopped.stderr))
    except Exception as e:record['errors'].append(repr(e))
    def confirm_removed():
        deadline=time.monotonic()+10
        while True:
            inspected=subprocess.run(['docker','inspect',owned],capture_output=True,text=True,timeout=5)
            if inspected.returncode:
                if 'no such' in inspected.stderr.lower():return True
                raise RuntimeError(('removal inspect failed',inspected.stderr))
            if time.monotonic()>=deadline:return False
            time.sleep(.1)
    try:
        record['removed']=confirm_removed()
    except Exception as e:record['errors'].append(repr(e))
    if not record['removed']:
        try:
            # Exact verified ID only; fallback if stop or automatic removal failed.
            removed=subprocess.run(['docker','rm','--force',owned],capture_output=True,text=True,timeout=15)
            if removed.returncode and 'no such' not in removed.stderr.lower() and 'already in progress' not in removed.stderr:
                record['errors'].append(('docker rm',removed.stderr))
        except Exception as e:record['errors'].append(repr(e))
        try:record['removed']=confirm_removed()
        except Exception as e:record['errors'].append(repr(e))
    if not record['removed']:record['errors'].append('container removal unconfirmed')
    return record

def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('binary',type=pathlib.Path);ap.add_argument('--output',required=True,type=pathlib.Path);args=ap.parse_args()
    binary=args.binary.resolve();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    pgport=port();name='proxy-cache-2pc-'+secrets.token_hex(12);owned=None;proxy=None;reports=[];cleanup=[];proxy_cleanup=[];binary_runs=[];initial_digest=None;backend_bound=None;ownership_checks=[]
    def check(label,fn):
        try:
            before=backend_identity(owned,name,pgport)
            assert before==backend_bound and proxy.poll() is None and owns_listener(proxy.pid,px),'fixture identity changed before case'
            detail=fn()
            after=backend_identity(owned,name,pgport)
            assert after==backend_bound and proxy.poll() is None and owns_listener(proxy.pid,px),'fixture identity changed after case'
            ownership_checks.append({'case':label,'before':before,'after':after,'proxy_pid':proxy.pid})
            reports.append({'case':label,'pass':True,'detail':detail})
        except Exception as e:reports.append({'case':label,'pass':False,'error':repr(e)})
        print(json.dumps(reports[-1]),flush=True)
    try:
        initial_digest=binary_digest(binary)
        image='postgres:18.4-bookworm'
        owned=subprocess.check_output(['docker','run','-d','--rm','--pull=never','--name',name,'--label','proxy-cache-2pc-owned='+name,'--network','host','--memory','1g','--memory-swap','1g','--cpus','2','--pids-limit','256','--tmpfs','/var/lib/postgresql:rw,size=512m','-e','POSTGRES_USER=audit','-e','POSTGRES_DB=audit','-e','POSTGRES_HOST_AUTH_METHOD=trust',image,'postgres','-c','listen_addresses=127.0.0.1','-c',f'port={pgport}','-c',f'cluster_name={name}','-c','max_prepared_transactions=16','-c','shared_buffers=64MB'],text=True,timeout=60).strip()
        deadline=time.monotonic()+60
        while True:
            try:
                with connect(pgport) as s:version=query(s,'SHOW server_version')[0]
                break
            except (OSError,EOFError,RuntimeError):
                if time.monotonic()>deadline:raise
                time.sleep(.25)
        assert version.startswith('18.4'),version
        # initdb temporary server must have exited before continuing.
        time.sleep(2)
        with connect(pgport) as s:assert int(query(s,'SHOW max_prepared_transactions')[0])>=16
        backend_bound=backend_identity(owned,name,pgport)
        for tr in [False,True]:
            px,admin=port(),port();logpath=out/f'proxy-tr-{tr}.log';cfg=out/f'proxy-tr-{tr}.toml'
            cfg.write_text(f'listen_address="127.0.0.1:{px}"\nadmin_address="127.0.0.1:{admin}"\ntr_enabled={str(tr).lower()}\n[cache]\nenabled=true\nttl_secs=3600\n[[nodes]]\nhost="127.0.0.1"\nport={pgport}\nrole="primary"\n')
            with logpath.open('w') as log:
                launch_digest=binary_digest(binary)
                assert launch_digest==initial_digest,'binary changed before launch'
                proxy=subprocess.Popen([str(binary),'--config',str(cfg),'--log-level','heliosdb_proxy=warn,helios::cache=debug'],stdout=log,stderr=subprocess.STDOUT)
                deadline=time.monotonic()+20
                while True:
                    try:
                        with connect(px) as s:query(s,'SELECT 1')
                        if proxy.poll() is not None or not owns_listener(proxy.pid,px):raise RuntimeError('owned proxy listener not ready')
                        break
                    except (OSError,EOFError,RuntimeError):
                        if proxy.poll() is not None or time.monotonic()>deadline:raise
                        time.sleep(.1)
                executed_digest=binary_digest(pathlib.Path(f'/proc/{proxy.pid}/exe'))
                assert executed_digest==initial_digest,'executed binary differs from frozen input'
                binary_runs.append({'tr':tr,'pid':proxy.pid,'prelaunch_sha256':launch_digest,'executed_sha256':executed_digest})
                def hit_count():return logpath.read_text().count('cache hit')
                def run_case(mode,prepare_proxy):
                    table='cache_2pc_'+secrets.token_hex(5);gid='g_'+secrets.token_hex(5)
                    with contextlib.ExitStack() as stack:
                        direct=stack.enter_context(connect(pgport));reader=stack.enter_context(connect(px));writer=stack.enter_context(connect(px if prepare_proxy else pgport));committer=stack.enter_context(connect(px))
                        query(direct,f'CREATE TABLE {table} (v integer)');query(direct,f'INSERT INTO {table} VALUES (0)')
                        select=f'SELECT v FROM {table}'
                        assert query(reader,select)==['0'];assert query(reader,select)==['0']
                        # Independent proof that the warmed query is really cached.
                        query(direct,f'UPDATE {table} SET v=9');assert query(reader,select)==['0'],'cache admission missing'
                        query(writer,'BEGIN');query(writer,f'UPDATE {table} SET v=1');query(writer,f"PREPARE TRANSACTION '{gid}'")
                        # Directly visible 9 distinguishes cache preservation from a
                        # miss that merely refetches the unchanged pre-commit row.
                        assert query(direct,select)==['9']
                        assert query(reader,select)==['0'],'PREPARE must preserve the cached entry'
                        before_sync_rows=[]
                        def verify_before_sync():
                            got=query(reader,select);actual=query(direct,select)
                            before_sync_rows.append({'proxy':got,'backend':actual})
                            assert got==actual==['1'],{'error':'Flush commit must invalidate before Sync','proxy':got,'backend':actual}
                        before=hit_count();query(committer,f"COMMIT PREPARED '{gid}'",mode,before_sync=verify_before_sync if mode=='extended_flush' else None)
                        got=query(reader,select);actual=query(direct,select)
                        query(direct,f'DROP TABLE {table}')
                        assert actual==['1'],actual
                        assert got==actual,{'proxy':got,'backend':actual,'mode':mode,'tr':tr,'preparer_proxy':prepare_proxy}
                        return {'proxy':got,'backend':actual,'cache_hit_log_before_commit':before,'prepare_preserved_cached_zero_over_backend_nine':True,'before_sync_reads':before_sync_rows}
                for mode in ['simple','extended_sync','extended_flush']:
                    for prepare_proxy in [False,True]:check(f'tr={tr}/{mode}/prepare_proxy={prepare_proxy}',lambda m=mode,p=prepare_proxy:run_case(m,p))
                def negative_case():
                    table='cache_2pc_'+secrets.token_hex(5);gid='g_'+secrets.token_hex(5)
                    with connect(pgport) as direct,connect(px) as reader,connect(px) as writer:
                        query(direct,f'CREATE TABLE {table} (v integer)');query(direct,f'INSERT INTO {table} VALUES (0)');select=f'SELECT v FROM {table}'
                        query(reader,select);query(reader,select)
                        query(writer,'BEGIN');query(writer,f'UPDATE {table} SET v=2');query(writer,f"PREPARE TRANSACTION '{gid}'")
                        before=hit_count();query(writer,f"ROLLBACK PREPARED '{gid}'");assert query(reader,select)==['0'];assert hit_count()>before,'rollback should preserve cached entry'
                        before=hit_count();query(writer,"COMMIT PREPARED 'missing_owned_fixture_gid'",expect_error=True);assert query(reader,select)==['0'];assert hit_count()>before,'failed commit should preserve cached entry'
                        query(direct,f'DROP TABLE {table}')
                    return 'rollback and failed commit preserve cached entry'
                check(f'tr={tr}/negative_controls',negative_case)
                stopped=stop_proxy(proxy);proxy_cleanup.append(stopped)
                if not stopped['reaped']:raise RuntimeError('proxy cleanup failed')
                proxy=None
    except Exception as e:reports.append({'case':'infrastructure','pass':False,'error':repr(e)})
    finally:
        # Every cleanup and identity check is independent: one failure must not
        # skip the other owned resource or suppress the durable result.
        if proxy is not None:
            try:proxy_cleanup.append(stop_proxy(proxy))
            except Exception as e:proxy_cleanup.append({'pid':proxy.pid,'reaped':False,'errors':[repr(e)]})
        try:cleanup.append(stop_container(owned,name))
        except Exception as e:cleanup.append({'id':owned,'removed':False,'errors':[repr(e)]})
        final_digest=None;identity_errors=[];script_digest=None
        try:final_digest=binary_digest(binary)
        except Exception as e:identity_errors.append(repr(e))
        try:script_digest=binary_digest(pathlib.Path(__file__))
        except Exception as e:identity_errors.append(repr(e))
        identity_ok=initial_digest is not None and initial_digest==final_digest and len(binary_runs)==2 and not identity_errors
        result={'binary':str(binary),'binary_sha256':initial_digest,'final_binary_sha256':final_digest,'binary_runs':binary_runs,'identity_errors':identity_errors,'reports':reports,'cleanup':cleanup,'proxy_cleanup':proxy_cleanup,'script_sha256':script_digest,'backend_identity':backend_bound,'ownership_checks':ownership_checks,
                'pass':len(reports)==14 and all(r['pass'] for r in reports) and identity_ok and len(cleanup)==1 and all(c['removed'] and not c['errors'] for c in cleanup) and len(proxy_cleanup)==2 and all(c['reaped'] and not c['errors'] for c in proxy_cleanup)}
        (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    return 0 if result['pass'] else 1
if __name__=='__main__':raise SystemExit(main())
