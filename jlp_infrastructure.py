# jlp main table headers list creation
#make_main_headers_list.py

import pandas as pd

def sql_load_labels_on_sf(sql_file):
    from snowflake.connector import connect
    from codecs import open

    conn = connect(
            user='jlp_testrun',
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

# sql_load_labels_on_sf(r'jlp_infrastructure_1.sql')

print('done2')

# edit file path to where your source data is stored
df_main = pd.read_csv('JohnLewisCSVCutDown.csv')


main_header_list = list(df_main.columns)

final_list = []
for item in main_header_list:
    final_list.append(item + ' varchar(50),')

final_list[-1] = final_list[-1][:-1]

with open("main_headers.txt", "w") as file: 
    for item in final_list: 
        file.write(str(item) + "\n")

# main_headers.txt
# headers from jlp labels csv and create jlp labels and jlp labels transposed table into snowflake

def labels_header():
    '''
    function documentation
    '''
    import pandas as pd
    df_labels = pd.read_csv(r'JLPLabels.csv', encoding='mbcs') #encoding='unicode_escape'

    labels_header_list = list(df_labels.columns)

    final_list = []
    for item in labels_header_list:
        final_list.append('"' + item + '"' + ' varchar(255),')

    final_list[-1] = final_list[-1][:-1]

    with open("label_headers.txt", "w") as file1: 
        for item in final_list: 
            file1.write(str(item) + "\n")

# Generate headers list for create labels_transposed table
def labels_transposed_header():
    '''
    function documentation
    '''
    transposed_header_list = []
    counter = 1
    for counter in range(102):
        message = fr'"{counter}" string,'
        transposed_header_list.append('"' + str(counter) + '"' + 'string,')
        counter = counter + 1

    with open("label_transposed_headers.txt", "w") as file2: 
        for item in transposed_header_list: 
            file2.write(str(item) + "\n")

labels_header()
labels_transposed_header()


# Formats and creates trimmed jlp_labels csv file
def format_create_trimmed_labels():
    import pandas as pd

    filename = r'JLPLabels.csv'
    df_chunk = pd.read_csv(filename, encoding = 'ISO-8859-1', engine = 'python')
    for column in df_chunk:
        df_chunk[column] = df_chunk[column].astype('string')
    transposed_df = df_chunk.transpose()
    transposed_df = transposed_df.reset_index()

    new_column_names = [*range(1,101,1)]
    
    # iterate over the list and rename each column
    for i, new_name in enumerate(new_column_names):
        transposed_df = transposed_df.rename(columns={transposed_df.columns[i]: str(new_name)})
    
    for column in transposed_df.columns[0:1]:
        transposed_df[column] = transposed_df[column].str.slice(0,255)

    df_chunk = transposed_df.transpose()

    df_chunk.columns = df_chunk.iloc[0]
    df_chunk = df_chunk[1:]
    df_chunk = df_chunk.set_index(['Status. Participant status'])
    df_chunk.to_csv('trimmed_labels.csv', sep=',',encoding = 'utf-8')

# Formats and creates trimmed transposed_jlp_labels csv file
def format_create_trimmed_labels_trans():
    import pandas as pd
    filename = r'JLPLabels.csv'
    df_chunk = pd.read_csv(filename, encoding = 'ISO-8859-1', engine = 'python')
    for column in df_chunk:
        df_chunk[column] = df_chunk[column].astype('string')
    transposed_df = df_chunk.transpose()
    transposed_df = transposed_df.reset_index()

    new_column_names = [*range(1,101,1)]
    
# iterate over the list and rename each column
    for i, new_name in enumerate(new_column_names):
        transposed_df = transposed_df.rename(columns={transposed_df.columns[i]: str(new_name)})
    for column in transposed_df.columns[0:]:
        transposed_df[column] = transposed_df[column].str.slice(0,255)

    transposed_df.to_csv('trimmed_labels_transposed.csv', sep=',',encoding = 'utf-8')

# Formats and creates full transposed jlp_labels csv file




# Formats and creates full jlp_labels csv file
def format_create_detailed_labels():
    import pandas as pd

    filename = r'JLPLabels.csv'
    df_chunk = pd.read_csv(filename, encoding = 'ISO-8859-1', engine = 'python')
    for column in df_chunk:
        df_chunk[column] = df_chunk[column].astype('string')
    transposed_df = df_chunk.transpose()
    transposed_df = transposed_df.reset_index()

    transposed_df.insert(loc=1,column = 'newcol', value = ['' for i in range (transposed_df.shape[0])])

    new_column_names = [*range(1,102,1)]
    
    # iterate over the list and rename each column
    for i, new_name in enumerate(new_column_names):
        transposed_df = transposed_df.rename(columns={transposed_df.columns[i]: str(new_name)})
    
    transposed_df['2'] = transposed_df['1']

    for column in transposed_df.columns[0:1]:
        transposed_df[column] = transposed_df[column].str.slice(0,255)
    for column in transposed_df.columns[1:2]:
        transposed_df[column] = transposed_df[column].str.slice(255,500)

    df_chunk = transposed_df.transpose()
    #df_chunk1 = df_chunk.drop(index=df_chunk.index[0], axis = 0, inplace = True)
    #df_chunk1 = df_chunk.iloc[1:]
    df_chunk.columns = df_chunk.iloc[0]
    df_chunk = df_chunk[1:]
    df_chunk = df_chunk.set_index(['Status. Participant status'])
    df_chunk.to_csv('detailed_labels.csv', sep=',',encoding = 'utf-8')

# Formats and creates full jlp_labels_trans csv file
def format_create_detailed_labels_trans():
    import pandas as pd

    filename = r'JLPLabels.csv'
    df_chunk = pd.read_csv(filename, encoding = 'ISO-8859-1', engine = 'python')
    for column in df_chunk:
        df_chunk[column] = df_chunk[column].astype('string')
    transposed_df = df_chunk.transpose()
    transposed_df = transposed_df.reset_index()

    transposed_df.insert(loc=1,column = 'newcol', value = ['' for i in range (transposed_df.shape[0])])

    new_column_names = [*range(1,102,1)]
    
    # iterate over the list and rename each column
    for i, new_name in enumerate(new_column_names):
        transposed_df = transposed_df.rename(columns={transposed_df.columns[i]: str(new_name)})

    transposed_df['2'] = transposed_df['1']

    for column in transposed_df.columns[0:1]:
        transposed_df[column] = transposed_df[column].str.slice(0,255)
    for column in transposed_df.columns[1:2]:
        transposed_df[column] = transposed_df[column].str.slice(255,500)

    transposed_df.to_csv('detailed_labels_transposed.csv', sep=',',encoding = 'utf-8')

print('python part done')
# connect jlp_infrastructure_1.sql

sql_load_labels_on_sf(r'jlp_infrastructure_2.sql')

print('sql file done')