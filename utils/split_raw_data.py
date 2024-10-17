import pandas as pd
import numpy as np

folder = './tpch_data/tcph2data/'

files_to_split = ['h_lineitem', 'h_order']
parts_to_split = [8, 2]

for file, parts_num in zip(files_to_split, parts_to_split):
    filepath = folder + file + '.dsv'

    df = pd.read_csv(filepath, sep="\|\*\|", engine='python')
    split_dfs = np.array_split(df, parts_num)

    for i, df_i in enumerate(split_dfs):
        df_i.to_csv(folder + file + f'_{i + 1}.dsv')