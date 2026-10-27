"""AWU SQLite -> PostgreSQL full-refresh ETL (only existing awu_import tables)."""
import argparse
import getpass
import os
import sqlite3
from pathlib import Path
from urllib.parse import quote

import psycopg
from psycopg import sql

SCHEMA = 'awu_import'


def main():
    parser = argparse.ArgumentParser(description='Refresh AWU PostgreSQL copy from GameData.db')
    parser.add_argument('--sqlite', required=True, help='Path to the actual GameData.db')
    parser.add_argument('--dry-run', action='store_true', help='Inspect source without changing PostgreSQL')
    args = parser.parse_args()
    path = Path(args.sqlite).resolve()
    if not path.is_file():
        raise SystemExit(f'File not found: {path}')
    uri = 'file:' + quote(path.as_posix(), safe='/:') + '?mode=ro'
    with sqlite3.connect(uri, uri=True) as source:
        source.execute('PRAGMA query_only = ON')
        names = [r[0] for r in source.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        source_cols = {t: [r[1] for r in source.execute(f'PRAGMA table_info("{t}")')] for t in names}
        source_counts = {t: source.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in names}
        print(f'SQLite: {path}\nTables: {len(names)}; rows: {sum(source_counts.values())}')
        if args.dry_run:
            for t in names:
                print(f'  {t}: {source_counts[t]}')
            return
        password = os.getenv('PGPASSWORD') or getpass.getpass('PostgreSQL password (hidden): ')
        with psycopg.connect(host=os.getenv('PGHOST', '127.0.0.1'), port=os.getenv('PGPORT', '5432'),
                             dbname='awu_lab', user=os.getenv('PGUSER', 'postgres'),
                             password=password, connect_timeout=8) as target:
            with target.transaction():
                with target.cursor() as cur:
                    cur.execute('SELECT current_database()')
                    if cur.fetchone()[0] != 'awu_lab':
                        raise RuntimeError('Refusing to load outside awu_lab')
                    cur.execute('SELECT pg_advisory_xact_lock(76251091)')
                    cur.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type=%s', (SCHEMA, 'BASE TABLE'))
                    dest_names = {r[0] for r in cur.fetchall()}
                    if dest_names != set(names):
                        raise RuntimeError(f'Table mismatch. Missing in PostgreSQL: {set(names)-dest_names}; extra in PostgreSQL: {dest_names-set(names)}')
                    for t in names:
                        cur.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position', (SCHEMA, t))
                        cols = [r[0] for r in cur.fetchall()]
                        if cols != source_cols[t]:
                            raise RuntimeError(f'Column mismatch in {t}: SQLite={source_cols[t]}, PostgreSQL={cols}')
                    # Foreign-key edges: child -> parent, restricted to the imported schema.
                    cur.execute('''SELECT child.relname, parent.relname
                                   FROM pg_constraint fk
                                   JOIN pg_class child ON child.oid=fk.conrelid
                                   JOIN pg_namespace nc ON nc.oid=child.relnamespace
                                   JOIN pg_class parent ON parent.oid=fk.confrelid
                                   JOIN pg_namespace np ON np.oid=parent.relnamespace
                                   WHERE fk.contype='f' AND nc.nspname=%s AND np.nspname=%s''', (SCHEMA, SCHEMA))
                    deps = {t: set() for t in names}
                    for child, parent in cur.fetchall():
                        deps[child].add(parent)
                    order = []
                    while deps:
                        ready = sorted(t for t, parents in deps.items() if not parents)
                        if not ready:
                            raise RuntimeError('Cyclic foreign keys: cannot safely full-refresh with DELETE')
                        for t in ready:
                            order.append(t)
                            deps.pop(t)
                        for parents in deps.values():
                            parents.difference_update(ready)
                    # Delete children first, then insert parents first. No CASCADE, no DROP.
                    for t in reversed(order):
                        cur.execute(sql.SQL('DELETE FROM {}.{}').format(sql.Identifier(SCHEMA), sql.Identifier(t)))
                    for t in order:
                        cols = source_cols[t]
                        if not cols:
                            continue
                        query = sql.SQL('INSERT INTO {}.{} ({}) VALUES ({})').format(
                            sql.Identifier(SCHEMA), sql.Identifier(t),
                            sql.SQL(', ').join(map(sql.Identifier, cols)),
                            sql.SQL(', ').join(sql.Placeholder() for _ in cols))
                        source_cur = source.execute(f'SELECT * FROM "{t}"')
                        while True:
                            rows = source_cur.fetchmany(500)
                            if not rows:
                                break
                            cur.executemany(query, rows)
                        print(f'Loaded {t}: {source_counts[t]} rows')
                    # Align owned identity/serial sequences to loaded values.
                    cur.execute('''SELECT t.relname, a.attname, pg_get_serial_sequence(format('%I.%I', n.nspname, t.relname), a.attname)
                                   FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
                                   JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum>0 AND NOT a.attisdropped
                                   WHERE n.nspname=%s AND t.relkind='r' ''', (SCHEMA,))
                    for table, col, sequence in cur.fetchall():
                        if sequence:
                            cur.execute(sql.SQL('SELECT MAX({}) FROM {}.{}').format(sql.Identifier(col), sql.Identifier(SCHEMA), sql.Identifier(table)))
                            maxval = cur.fetchone()[0]
                            if maxval is None:
                                cur.execute('SELECT setval(%s, 1, false)', (sequence,))
                            else:
                                cur.execute('SELECT setval(%s, %s, true)', (sequence, maxval))
                    for t in names:
                        cur.execute(sql.SQL('SELECT count(*) FROM {}.{}').format(sql.Identifier(SCHEMA), sql.Identifier(t)))
                        actual = cur.fetchone()[0]
                        if actual != source_counts[t]:
                            raise RuntimeError(f'Count mismatch in {t}: expected {source_counts[t]}, got {actual}')
            print('SUCCESS: full refresh committed in awu_lab.awu_import.')


if __name__ == '__main__':
    main()
