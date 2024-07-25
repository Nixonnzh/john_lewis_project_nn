def jlp_label_codes():
    import pandas as pd

    jlp_labels_df = pd.read_csv('JLPLabels.csv',encoding='latin1')

    jlp_label_codes = jlp_labels_df.melt()

    jlp_lc = jlp_label_codes.dropna()

    jlp_lc.to_csv('label_codes.csv', index=False, header=False)

jlp_label_codes()

