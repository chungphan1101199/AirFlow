import pandas as pd
import os
from datetime import datetime
import dateutil.parser as parser
import sys
datatype = sys.argv[1]
#-- đọc file cấu hình
def put_file_hdfs(code=None):
    os.chdir(os.path.expanduser('/ftp/sourcecode'))
    df = pd.read_csv('config.csv')
    os.chdir(os.path.expanduser('/ftp'))    
    if not os.path.exists('log/'):
        return
#for từng nguồn file
    if code =='ALL':
        gc = 'ALL SOURCE'
    else:
        gc = code
        df = df[df['group_code']== code]
        
    print('START PUT HDFS FILE: GROUP_CODE:'+ gc)
    
    for i,row in df.iterrows():
                
        os.chdir(os.path.expanduser('/ftp'))  
        print('Start_putfile from:'+row.group_code+ ' id: '+ str(row.ftp_source_id))
        local_file_list = pd.DataFrame()
        if os.path.isfile('/ftp/log/'+row.group_code+'_'+str(row.ftp_source_id)+'.csv'):
            local_file_list = pd.read_csv('/ftp/log/'+row.group_code+'_'+str(row.ftp_source_id)+'.csv')
        else:
            continue
        if local_file_list.empty:
            continue
            
        if 'hdfs_time' not in local_file_list.columns:
            local_file_list['hdfs_time']  = None
            
        if not os.path.exists(os.path.expanduser('/ftp/'+row.group_code+'/'+str(row.ftp_source_id)+'/done')):
            os.makedirs(os.path.expanduser('/ftp/'+row.group_code+'/'+str(row.ftp_source_id)+'/done'))     
        local_file_list['hdfs_done'] = (local_file_list ['hdfs_time'].isnull() == False).astype(int)
        for index, file in local_file_list.iterrows():
            if file['hdfs_done'] == 1:
                continue
            file_dir = os.path.expanduser('/ftp/'+row['group_code']+'/'+str(row['ftp_source_id'])+'/'+file['filename'])
            file_success = os.path.expanduser('/ftp/'+row['group_code']+'/'+str(row['ftp_source_id'])+'/done/'+file['filename'])
            file_dest = row.group_code+'/'+str(row.ftp_source_id)
            if not os.path.isfile(file_dir):
                local_file_list.at[index, 'hdfs_done'] = 1
                local_file_list.at[index, 'hdfs_time'] = datetime.now()
                continue
            
            #kiem tra folder duoc tao chua
            try:
                subprocess.check_call(['hdfs', 'dfs', '-test', '-d', row.group_code])
            except:
                try:
                    subprocess.check_call(['hdfs', 'dfs', '-mkdir', row.group_code])
                except:
                    print('Error_hdfs mkdir:'+row.group_code)
                    continue
                    
            try:
                subprocess.check_call(['hdfs', 'dfs', '-test', '-d', row.group_code+'/'+str(row.ftp_source_id)])
            except:
                try:
                    subprocess.check_call(['hdfs', 'dfs', '-mkdir', row.group_code+'/'+str(row.ftp_source_id)])
                except:
                    print('Error_hdfs mkdir:'+row.group_code+'/'+str(row.ftp_source_id))
                    continue
              #put file va chuyen vao success       
            try:
                subprocess.check_call(['hdfs', 'dfs', '-put','-f', file_dir, file_dest])
                shutil.move(file_dir, file_success)
                local_file_list.at[index, 'hdfs_done'] = 1
                local_file_list.at[index, 'hdfs_time'] =datetime.now()   
                print('put hdfs:'+file_dir +' to:'+ file_dest)
            except:
                print('Error_putfile hdfs from:'+file_dir)
                continue
          
        local_file_list.to_csv(os.path.expanduser('/ftp/log/'+row.group_code+'_'+str(row.ftp_source_id)+'.csv'),index=False, date_format= '%Y-%m-%d %H:%M:%S')
        print('End_putfile hdfs from:'+row.group_code+ ' id: '+ str(row.ftp_source_id))
        
    print('FINISHED PUT HDFS FILE: GROUP_CODE:'+ gc)
    return


if __name__ == "__main__":
    put_file_hdfs(datatype)