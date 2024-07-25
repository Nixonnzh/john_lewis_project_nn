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


sql_load_labels_on_sf(r'jlp_load_1.sql')
# create label codes csv in python and load to snowflake - jlp_label_codes.py

def jlp_label_codes():
    import pandas as pd

    jlp_labels_df = pd.read_csv('JLPLabels.csv',encoding='latin1')

    jlp_label_codes = jlp_labels_df.melt()

    jlp_lc = jlp_label_codes.dropna()

    jlp_lc.to_csv('label_codes.csv', index=False ,header=False)

jlp_label_codes()

# jlp_load_2.sql
sql_load_labels_on_sf(r'jlp_load_2.sql')