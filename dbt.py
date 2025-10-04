import argparse
import json
import os
import shutil
import subprocess


def run(cmd, vars=None):
    env = os.environ.copy()
    if vars:
        env.update(vars)
    print('$', ' '.join(cmd))
    subprocess.run(cmd, check=True, env=env)


def db_get(key):
    if key not in DB:
        raise ValueError(f'db.{key} is not defined in config')
    DB[key]['host'] = DB[key].get('host', DEFAULT.get('host', 'localhost'))
    DB[key]['port'] = DB[key].get('port', DEFAULT.get('port', 5432))
    DB[key]['user'] = DB[key].get('user', DEFAULT.get('user', 'postgres'))
    if 'name' not in DB[key] and 'name' not in DEFAULT:
        raise ValueError(f'db.{key}.name is not defined in config')
    DB[key]['name'] = DB[key].get('name', DEFAULT.get('name'))
    if 'password' not in DB[key]:
        raise ValueError(f'db.{key}.password is not defined in config')


def db_copy(args):
    src = args.source
    dst = args.target
    use_cache = args.use_cache

    dump = os.path.join(DUMP, src)
    dump_found = os.path.isdir(dump)

    if use_cache and dump_found:
        pass
    else:
        if dump_found:
            shutil.rmtree(dump)
        elif use_cache:
            print('>>> DUMP NOT FOUND')

        os.makedirs(DUMP, exist_ok=True)

        db_get(src)

        run([
            'pg_dump',
            '--no-owner',
            '--no-privileges',
            '--format=directory',
            f'--host={DB[src]['host']}',
            f'--port={DB[src]['port']}',
            f'--username={DB[src]['user']}',
            f'--dbname={DB[src]['name']}',
            f'--jobs={JOBS}',
            f'--file={dump}'
        ], vars={'PGPASSWORD': DB[src]['password']})

    db_get(dst)

    run([
        'dropdb',
        '--if-exists',
        f'--host={DB[dst]['host']}',
        f'--port={DB[dst]['port']}',
        f'--username={DB[dst]['user']}',
        f'{DB[dst]['name']}'
    ], vars={'PGPASSWORD': DB[dst]['password']})

    run([
        'createdb',
        f'--host={DB[dst]['host']}',
        f'--port={DB[dst]['port']}',
        f'--username={DB[dst]['user']}',
        f'{DB[dst]['name']}'
    ], vars={'PGPASSWORD': DB[dst]['password']})

    run([
        'pg_restore',
        '--exit-on-error',
        f'--host={DB[dst]['host']}',
        f'--port={DB[dst]['port']}',
        f'--username={DB[dst]['user']}',
        f'--dbname={DB[dst]['name']}',
        f'--jobs={JOBS}',
        f'{dump}'
    ], vars={'PGPASSWORD': DB[dst]['password']})


def db_connect(args):
    key = args.target

    db_get(key)

    run([
        'psql',
        f'--host={DB[key]['host']}',
        f'--port={DB[key]['port']}',
        f'--username={DB[key]['user']}',
        f'--dbname={DB[key]['name']}'
    ], vars={'PGPASSWORD': DB[key]['password']})


def db_run(args):
    key = args.target
    file = args.file

    db_get(key)

    run([
        'psql',
        f'--host={DB[key]['host']}',
        f'--port={DB[key]['port']}',
        f'--username={DB[key]['user']}',
        f'--dbname={DB[key]['name']}',
        f'--file={file}'
    ], vars={'PGPASSWORD': DB[key]['password']})


def main():
    global DUMP, JOBS, DEFAULT, DB

    BASE = os.path.dirname(os.path.abspath(__file__))
    DUMP = os.path.join(BASE, 'dump')
    CONF = os.path.join(BASE, 'conf.json')

    try:
        with open(CONF) as f:
            conf = json.load(f)
    except FileNotFoundError:
        conf = {}

    JOBS = conf.get('jobs', 16)
    DEFAULT = conf.get('default', {})
    DB = conf.get('db', {})

    parser = argparse.ArgumentParser(prog='dbt')
    subparsers = parser.add_subparsers(dest='command', required=True)

    # copy
    parser_copy = subparsers.add_parser('copy', help='Copy database')
    parser_copy.add_argument('--from', dest='source', required=True, help='Source DB')
    parser_copy.add_argument('--to', dest='target', required=True, help='Target DB')
    parser_copy.add_argument('--use-cache', dest='use_cache', action='store_true', help='Use existing dump if available')
    parser_copy.set_defaults(func=db_copy)

    # connect
    parser_connect = subparsers.add_parser('connect', help='Connect to database')
    parser_connect.add_argument('--to', dest='target', required=True, help='Target DB')
    parser_connect.set_defaults(func=db_connect)

    # run
    parser_run = subparsers.add_parser('run', help='Run SQL file on database')
    parser_run.add_argument('--on', dest='target', required=True, help='Target DB')
    parser_run.add_argument('--file', dest='file', required=True, help='SQL file to run')
    parser_run.set_defaults(func=db_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
