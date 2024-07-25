# Generate headers list for create labels table
def labels_header():
    '''
    function documentation
    '''
    import pandas as pd
    df_labels = pd.read_csv(r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\data\JLPLabels.csv', encoding='mbcs') #encoding='unicode_escape'

    labels_header_list = list(df_labels.columns)

    final_list = []
    for item in labels_header_list:
        final_list.append('"' + item + '"' + ' varchar(255),')

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

####
# SQL file (load_jlp_label_csv_into.sql & load_transposed_jlp_label.sql to create empty labels tables and load csv files)
from snowflake.connector import connect
from codecs import open

conn = connect(
        user='nixonng',
        password='********',
        account='hhcdkol-xn67836',
        warehouse='BIG_WAREHOUSE',
        database='JLP_BPS',
        schema='initial_schema'
    )

sql_file = r"C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\git_backup\test_load_jlp_labels.sql"

with open(sql_file, 'r', encoding='utf-8') as f:
    for cs in conn.execute_stream(f):
        for rt in cs:
            print(rt)

conn.close()
print('done')
#
####

# Formats and creates trimmed jlp_labels csv file
def format_create_trimmed_labels():
    import pandas as pd

    filename = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\data\JLPLabels.csv'
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
    filename = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\data\JLPLabels.csv'
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
def format_create_detailed_labels_trans():
    import pandas as pd

    filename = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\data\JLPLabels.csv'
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



# Formats and creates full jlp_labels csv file
def format_create_detailed_labels():
    import pandas as pd

    filename = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\data\JLPLabels.csv'
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


format_create_trimmed_labels()
format_create_trimmed_labels_trans()
format_create_detailed_labels()
format_create_detailed_labels_trans()

# Test source csv files are identical to uploaded tables on sf
def source_load_sf_test():
    import pandas as pd
    # Uploading jlp_labels tables (source & snowflake)
    filename1 = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\project_file\source_jlp_labels.csv'
    ctest_df1 = pd.read_csv(filename1, encoding = 'ISO-8859-1', engine = 'python')
    filename2 = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\project_file\test_jlp_labels.csv'
    ctest_df2 = pd.read_csv(filename2, encoding = 'ISO-8859-1', engine = 'python')
    # Uploading transposed_jlp_labels tables (source & snowflake)
    filename3 = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\project_file\source_transposed_jlp_labels.csv'
    ctest_df3 = pd.read_csv(filename3, encoding = 'ISO-8859-1', engine = 'python')
    filename4 = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\project_file\test_transposed_jlp_labels.csv'
    ctest_df4 = pd.read_csv(filename4, encoding = 'ISO-8859-1', engine = 'python')
    # Uploading JohnLewisCSVCutDown tables (source & snowflake)
    filename5 = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\data\JohnLewisCSVCutDown.csv'
    ctest_df5 = pd.read_csv(filename5, encoding = 'ISO-8859-1', engine = 'python')
    filename6 = r'C:\Users\NixonNg\OneDrive - Kubrick Group\Desktop\Kubrick Training Program\Week 14 (Client Project)\JLP\project_file\test_JohnLewisCSVCutDown.csv'
    ctest_df6 = pd.read_csv(filename6, encoding = 'ISO-8859-1', engine = 'python')
    # Testing merge dataframes for duplicates
    merge_df1 = pd.concat([ctest_df1,ctest_df2], axis=0)
    merge_df2 = pd.concat([ctest_df3,ctest_df4], axis=0)
    merge_df3 = pd.concat([ctest_df5,ctest_df6], axis=0)
    #display(merge_df1.drop_duplicates(keep=False))
    #display(merge_df2.drop_duplicates(keep=False))
    #display(merge_df3.drop_duplicates(keep=False))

source_load_sf_test()