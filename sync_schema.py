"""
Compare local vs cloud DB schemas and apply any missing columns/indexes to cloud.
"""
import sys
sys.path.insert(0, '.')
from database_connection import establish_local_database, establish_cloud_database

def get_tables(conn, db_name):
    cur = conn.cursor()
    cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE'", (db_name,))
    return {row[0] for row in cur.fetchall()}

def get_columns(conn, db_name, table):
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, column_type, is_nullable, column_default, extra, column_comment
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (db_name, table))
    return {row[0]: row for row in cur.fetchall()}

def get_create_table(conn, table):
    cur = conn.cursor()
    cur.execute(f"SHOW CREATE TABLE `{table}`")
    return cur.fetchone()[1]

def col_def(col_info):
    """Build ADD COLUMN definition string from information_schema row."""
    name, col_type, nullable, default, extra, comment = col_info
    defn = f"`{name}` {col_type}"
    if extra:
        defn += f" {extra}"
    if nullable == 'NO':
        defn += " NOT NULL"
    else:
        defn += " NULL"
    if default is not None:
        if default == 'CURRENT_TIMESTAMP' or default.upper().startswith('CURRENT_TIMESTAMP'):
            defn += f" DEFAULT {default}"
        else:
            defn += f" DEFAULT '{default}'"
    if comment:
        defn += f" COMMENT '{comment}'"
    return defn

def main():
    local = establish_local_database()
    cloud = establish_cloud_database()
    local_db = 'web_application'
    cloud_db = 'web_application'

    local_tables = get_tables(local, local_db)
    cloud_tables = get_tables(cloud, cloud_db)

    print(f"Local tables:  {len(local_tables)}")
    print(f"Cloud tables:  {len(cloud_tables)}")

    missing_tables = local_tables - cloud_tables
    extra_tables = cloud_tables - local_tables

    if missing_tables:
        print(f"\n=== Tables in local but NOT in cloud (need to create): ===")
        for t in sorted(missing_tables):
            print(f"  {t}")
    if extra_tables:
        print(f"\n=== Tables in cloud but NOT in local (info only): ===")
        for t in sorted(extra_tables):
            print(f"  {t}")

    common_tables = local_tables & cloud_tables
    alters = []

    print(f"\n=== Column diff for common tables ===")
    for table in sorted(common_tables):
        local_cols = get_columns(local, local_db, table)
        cloud_cols = get_columns(cloud, cloud_db, table)

        missing_cols = set(local_cols) - set(cloud_cols)
        extra_cols   = set(cloud_cols) - set(local_cols)
        changed_cols = []
        for col in set(local_cols) & set(cloud_cols):
            # compare type + nullable + default + extra
            l = local_cols[col]
            c = cloud_cols[col]
            if l[1] != c[1] or l[2] != c[2] or l[4] != c[4]:
                changed_cols.append(col)

        if missing_cols or changed_cols:
            print(f"\n  {table}:")
            if missing_cols:
                print(f"    Missing columns: {sorted(missing_cols)}")
            if changed_cols:
                for col in sorted(changed_cols):
                    l = local_cols[col]
                    c = cloud_cols[col]
                    print(f"    Changed: {col}  local=({l[1]}, nullable={l[2]}, extra={l[4]})  cloud=({c[1]}, nullable={c[2]}, extra={c[4]})")

            for col in sorted(missing_cols):
                defn = col_def(local_cols[col])
                alters.append(f"ALTER TABLE `{table}` ADD COLUMN {defn};")
            for col in sorted(changed_cols):
                defn = col_def(local_cols[col])
                alters.append(f"ALTER TABLE `{table}` MODIFY COLUMN {defn};")
        else:
            if extra_cols:
                print(f"\n  {table}: cloud has extra cols (local doesn't): {sorted(extra_cols)}")

    # Handle missing tables: generate CREATE TABLE from local
    create_stmts = []
    for table in sorted(missing_tables):
        ddl = get_create_table(local, table)
        # Strip AUTO_INCREMENT value to avoid issues
        import re
        ddl = re.sub(r' AUTO_INCREMENT=\d+', '', ddl)
        create_stmts.append(ddl + ';')

    print("\n\n=== DDL to apply to cloud ===")
    if not create_stmts and not alters:
        print("  Nothing to do — schemas are in sync.")
    else:
        for s in create_stmts:
            print(s)
        for s in alters:
            print(s)

    # Ask before applying
    if create_stmts or alters:
        ans = input("\nApply these changes to cloud? [y/N]: ").strip().lower()
        if ans == 'y':
            cur = cloud.cursor()
            for stmt in create_stmts + alters:
                print(f"  Executing: {stmt[:120]}")
                try:
                    cur.execute(stmt)
                    print("    OK")
                except Exception as e:
                    print(f"    ERROR: {e}")
            print("Done.")
        else:
            print("Aborted — no changes made.")

    local.close()
    cloud.close()

if __name__ == '__main__':
    main()
