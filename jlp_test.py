def sql_load_labels_on_sf(sql_file):
    from snowflake.connector import connect
    from codecs import open

    conn = connect(
            user='JLP_TESTRUN',
            password='Kubrick123',
            account='ed81217.uk-south.azure',
            warehouse='BIG_WAREHOUSE',
            database='JLP_BPS',
            schema='initial_schema'
        )

    with open(sql_file, 'r', encoding='utf-8') as f:
        for cs in conn.execute_stream(f):
            for rt in cs:
                print(rt)

    conn.close()
    print('done')


sql_load_labels_on_sf(r'jlp_load_2.sql')