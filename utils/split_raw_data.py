import pandas as pd
import numpy as np

folder = './tpch_data/tcph2data/'
clean_folder = './tpch_data/tcph_processed_data/'

files_to_split = ['h_lineitem', 'h_order', 'h_customer', 'h_nation', 'h_part', 'h_partsupp', 'h_supplier']
parts_to_split = [8, 2, 1, 1, 1, 1, 1]

for file, parts_num in zip(files_to_split, parts_to_split):
    filepath = folder + file + '.dsv'

    df = pd.read_csv(filepath, sep="|", engine='python', decimal=',')
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    split_dfs = np.array_split(df, parts_num)

    if len(split_dfs) > 1:
        for i, df_i in enumerate(split_dfs):
            df_i.to_csv(clean_folder + file + f'_{i + 1}.csv', index=False, sep=';', decimal='.')
    else:
        split_dfs[0].to_csv(clean_folder + file + f'.csv', index=False, sep=';', decimal='.')