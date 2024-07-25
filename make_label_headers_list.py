import pandas as pd

# edit file path to where your source data is stored
df_labels = pd.read_csv('LabelsTrimmed.csv', encoding='mbcs') #encoding='unicode_escape'

labels_header_list = list(df_labels.columns)

final_list = []
for item in labels_header_list:
    final_list.append('"' + item + '"' + ' varchar(255),')

final_list[-1] = final_list[-1][:-1]

with open("label_headers.txt", "w") as file2: 
    for item in final_list: 
        file2.write(str(item) + "\n")