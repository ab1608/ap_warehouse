import duckdb


def generate_dynamic_views(
    db_conn: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyConnection:
    tables = db_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    all_tables = [t[0] for t in tables]

    # Create buckets for different view categories
    buckets: dict[str, list] = {"committed": [], "ccdet": [], "actuals": []}

    for table in all_tables:
        if "commit" in table:
            buckets["committed"].append(table)
        elif "ccdet" in table:
            buckets["ccdet"].append(table)
        else:
            buckets["actuals"].append(table)

    # 4. Generate and execute CREATE VIEW statements
    view_names = {
        "committed": "committed_view",
        "ccdet": "cost_center_details_view",
        "actuals": "actuals_view",
    }

    for key, table_list in buckets.items():
        if not table_list:
            print(f"Skipping {view_names[key]} - no matching tables found.")
            continue

        union_query = " UNION ALL BY NAME ".join(
            [f"SELECT * FROM {t}" for t in table_list]
        )
        create_view_sql = f"CREATE OR REPLACE VIEW {view_names[key]} AS {union_query}"

        db_conn.execute(create_view_sql)
        print(f"Created {view_names[key]} with {len(table_list)} tables.")

    return db_conn
