import os
import logging
import pandas as pd
from datetime import date
import sys

def process_main(input_directory=None, output_directory=None):
    try:
        files = [file for file in os.listdir(input_directory) if file.lower().endswith(".xls")]
        # Process each file and save the results
        processed_dfs_nid = []
        processed_dfs_daily = []
        for xls_file in files:
            input_file_path = os.path.join(input_directory, xls_file)
            df = read_xls_file(input_file_path)
            if df is not None:
                # Process the data for the current file
                processed_df_nid = process_data_nid(df, xls_file)
                processed_df_daily = process_data_daily(df, xls_file)
                if processed_df_nid is not None and processed_df_daily is not None:
                    # Save the processed DataFrame for each file
                    processed_dfs_nid.append(processed_df_nid)
                    processed_dfs_daily.append(processed_df_daily)
                else:
                    print(f"Data processing failed for file: {input_file_path}") 
        save_combined_df_nid(processed_dfs_nid,input_directory, output_directory, "agile.merge" )
        save_combined_df_daily(processed_dfs_daily,input_directory, output_directory, "agile.merge.daily" )         
    except Exception as e:
        print(f"Error while processing data: {str(e)}")
        
def read_xls_file(input_path):
    try:
        # 直接使用 read_excel 函数读取 csv 文件
        df = pd.read_csv(input_path, encoding='utf-16le', sep='\t', dtype={'文章ID': str})
        print("read xls is ok：", input_path)
        return df
    except Exception as e:
        print(f"read xls error：{str(e)}")
        return None
    
def process_data_daily(df, xls_file):
    try: 
        df = df.dropna(subset=['文章ID'])# 去除 '文章ID' 列为空的行
        df = df[df['文章ID'] != '--']# 去除 '文章ID' 列为'--'的行
        df = df[df['文章ID'].str.isdigit()]# 去除不是纯数字的行
        df['提交时间'] = pd.to_datetime(df['提交时间'])
        df['提交时间日期'] = df['提交时间'].dt.strftime('%Y%m%d')
        df = df_add_commentscore(df)      
        if xls_file == 'a.xls' or xls_file == 'b.xls':
            df['资源类型'] = 'feed'
        elif xls_file == 'mv.a.xls' or xls_file == 'mv.b.xls':
            df['资源类型'] = 'mv'
        elif xls_file == 'dt.xls':
            df['资源类型'] = 'dt'
        return df
    except Exception as e:
        print(f"process_data_daily出错：{str(e)}")
        return None
def save_combined_df_daily(processed_dfs, input_directory, output_directory, file_prefix):
    if processed_dfs:
        # Combine all processed DataFrames into one big DataFrame
        combined_df = pd.concat(processed_dfs, ignore_index=True)
        df = combined_df
        # 修改映射
        resource_type_mapping = {
                'shortVideo': 'sv',
                'miniVideo': 'mv',
                # Add more mappings here if needed
        }
        # Replace the original resource types with their shortened versions
        df['资源类型'] = df['资源类型'].replace(resource_type_mapping)
        # 根据提交时间日期 进行分组和聚合操作
        grouped_df = df.groupby(['提交时间日期', '资源类型'], as_index=False).agg({
            '好评数': 'sum',  # 计算好评数
            '中评数': 'sum',  # 计算中评数
            '差评数': 'sum',  # 计算差评数
            '评价总数': 'sum',  # 计算评价总数
        })          
        # 计算好评率，设置好评率为0当评价总数为0
        grouped_df['好评率'] = grouped_df['好评数'] / grouped_df['评价总数'].where(grouped_df['评价总数'] != 0, 1)
        # Drop the '好评数', '中评数', and '差评数' columns
        grouped_df.drop(columns=['好评数', '中评数', '差评数'], inplace=True)
       # Move the '资源类型' column to the last position
        cols = grouped_df.columns.tolist()
        cols.remove('资源类型')
        cols.append('资源类型')
        grouped_df = grouped_df[cols]  
        combined_df=grouped_df
        # 保存到txt，输出结果
        output_filename = f"{file_prefix}.{os.path.basename(input_directory)}.txt"
        output_file_path = os.path.join(output_directory, output_filename)
        combined_df.to_csv(output_file_path, sep='\t', index=False, header=False)
        logging.info("Final output: " + output_file_path)
        print("Final output agile_daily: " + output_file_path)
    else:
        print("Data processing failed. No output file created.")

def process_data_nid(df, xls_file):
    try:
        df = df.dropna(subset=['文章ID'])# 去除 '文章ID' 列为空的行
        df = df[df['文章ID'] != '--']# 去除 '文章ID' 列为'--'的行
        df = df[df['文章ID'].str.isdigit()]# 去除不是纯数字的行
        df = df_add_commentscore(df) 
        # 根据文件名，选取所需的列，并按文章nid进行分组和聚合操作
        if xls_file == 'a.xls' or xls_file == 'b.xls':
            resource_type_mapping = {
                'shortVideo': 'sv',
                'miniVideo': 'mv',
                # Add more mappings here if needed
            }
            # Replace the original resource types with their shortened versions
            df['资源类型'] = df['资源类型'].replace(resource_type_mapping)
        elif xls_file == 'mv.a.xls' or xls_file == 'mv.b.xls':
            df['资源类型'] = 'mv'
        elif xls_file == 'dt.xls':
            df['资源类型'] = 'dt_immerse'
        return df
    except Exception as e:
        print(f"process_data_nid出错：{str(e)}")
        return None
def save_combined_df_nid(processed_dfs, input_directory, output_directory, file_prefix):
    if processed_dfs:
        # Combine all processed DataFrames into one big DataFrame
        combined_df = pd.concat(processed_dfs, ignore_index=True)
        df = combined_df
        # 根据文章id 和资源类型 聚合
        grouped_df = df.groupby(['文章ID', '资源类型'], as_index=False).agg({
            '好评数': 'sum',  # 计算好评数
            '中评数': 'sum',  # 计算中评数
            '差评数': 'sum',  # 计算差评数
            '评价总数': 'sum',  # 计算评价总数
        })
        # 计算好评率，设置好评率为0当评价总数为0
        grouped_df['好评率'] = grouped_df['好评数'] / grouped_df['评价总数'].where(grouped_df['评价总数'] != 0, 1)
        combined_df=grouped_df
        # 保存到txt，输出结果
        output_filename = f"{file_prefix}.{os.path.basename(input_directory)}.txt"
        output_file_path = os.path.join(output_directory, output_filename)
        combined_df.to_csv(output_file_path, sep='\t', index=False, header=False)
        logging.info("Final output: " + output_file_path)
        print('output dir is:',output_directory)
        print("Final output agile_nid: " + output_file_path)
    else:
        print("Data processing failed. No output file created.")

def df_add_commentscore(df):
    df['好评数'] = (df['评分1'] == 1).astype(int)
    df['中评数'] = (df['评分1'] == 2).astype(int)
    df['差评数'] = (df['评分1'] == 3).astype(int)   
    df['评价总数'] = 0 # 新增'评价总数'列，默认为0 
    df['评价总数'] = df[['好评数', '中评数', '差评数']].sum(axis=1)# 计算每个文章ID的评价总数
    return df

if __name__ == "__main__":
    # Accept command-line arguments
    print("you need to reset input and output dir,or using default dir")
    if len(sys.argv) == 3:
        input_directory = sys.argv[1]
        output_directory = sys.argv[2]
        print("done! you reseted input and output dir")
    else:
        print('done! using default dir')
        input_directory = "data/defaultinput"
        output_directory ="data/defaultoutput"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
      
    process_main(input_directory, output_directory)
    
