import pandas as pd

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

