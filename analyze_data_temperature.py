import argparse
import subprocess
import os
import glob
import sys
import pandas as pd
import xml.etree.ElementTree as et
import time
import json

ONE_DAY = 24 * 60 * 60 * 1000 # milliseconds
HOT = 7 * ONE_DAY
WARM = 30 * ONE_DAY
COLD = 90 * ONE_DAY

def parse_args():
    parser = argparse.ArgumentParser(description='Analyze data temparature in HDFS.')
    parser.add_argument('--fetch-image', action='store_true', help='Fetch fsimage before analyzing.')
    parser.add_argument('--fetch-dir', default='/tmp', help='Output directory for fetched fsimage.')
    parser.add_argument('--image-file', default='', help='FSimage file to analyze.')
    parser.add_argument('--convert-dir', default='/tmp', help='Output directory for converted fsimage.')
    parser.add_argument('--image-xml', default='', help='Converted FSimage file in XML format to analyze.')
    parser.add_argument('--dfs-path', required=True, help='DFS path to analyze.')

    return parser.parse_args()

def parse_fsimage(image_xml):
    print("Parsing {} ...".format(image_xml))
    xtree = et.parse(image_xml)
    xroot = xtree.getroot()

    inode_cols = ["id", "type", "name", "mtime", "atime", "numBytes"]
    inode_dir_cols = ["parent", "children"]

    x_inodes = xroot.find('INodeSection')
    inode_count = len(x_inodes)
    print("Total number of inodes: {}.".format(len(x_inodes)))

    x_inode_dirs = xroot.find('INodeDirectorySection')
    dir_count = len(x_inode_dirs)
    print("{} directories, {} files.".format(dir_count, inode_count-dir_count))

    print("\nBuilding inode data frame")
    df_inode = pd.DataFrame(columns=inode_cols)

    all_inodes = x_inodes.findall('inode')
    for node in all_inodes:
        s_id = node.find("id").text
        s_type = node.find("type").text
        s_name = node.find("name").text
        s_mtime = node.find("mtime").text

        s_atime = None
        s_numByte = 0
        if s_type == 'FILE':
            s_atime = node.find("atime").text
            x_blocks = node.find('blocks')
            if x_blocks is not None:
                for x_block in x_blocks.findall('block'):
                    s_bid = x_block.find('id').text
                    s_numByte += int(x_block.find('numBytes').text)

        df_inode = df_inode.append(pd.Series([s_id, s_type, s_name, s_mtime, s_atime, s_numByte],
                                         index=inode_cols), ignore_index=True)

    df_inode = df_inode.set_index('id')
    root_id = df_inode.iloc[0].name
    print(df_inode.shape)
    print("Root inode id: " + root_id)
    # print(df_inode.head(10))

    print("\nBuilding inode directory data frame")
    df_inode_dir = pd.DataFrame(columns=inode_dir_cols)
    all_inode_dirs = x_inode_dirs.findall('directory')
    for node in all_inode_dirs:
        s_parent = node.find("parent").text
        children = node.findall("child")
        children_str = []
        for child in children:
             children_str.append(child.text)
        s_children = ",".join(children_str)

        df_inode_dir = df_inode_dir.append(pd.Series([s_parent, s_children],
                                         index=inode_dir_cols), ignore_index=True)

    df_inode_dir = df_inode_dir.set_index('parent')
    print(df_inode_dir.shape)
    # print(df_inode_dir.head(10))

    return root_id, df_inode, df_inode_dir

def analyze_temperature(parent_id, df_inode, df_inode_dir, paths, temp_dic):
    path = paths.pop(0)
    children = df_inode_dir.loc[parent_id]['children'].split(',')
    path_id = None
    for child in children:
        child_name = df_inode.loc[child]['name']
        if child_name == path:
            path_id = child
            break

    if path_id is None:
        print("{}: No such file or directory".format(args.dfs_path))
        sys.exit(-1)

    if len(paths) > 0:
        # keep digging
        analyze_temperature(path_id, df_inode, df_inode_dir, paths, temp_dic)
    else:
        # Reached the last input directory layer, start actual analyzing
        analyze_inode_bfs([path_id], [args.dfs_path], df_inode, df_inode_dir, temp_dic)


def analyze_inode_bfs(dir_queue, curr_path_queue, df_inode, df_inode_dir, temp_dic):
    if not dir_queue:
        # queue is empty, BFS analysis done
        return

    inode_id = dir_queue.pop()
    current_path = curr_path_queue.pop()
    inode = df_inode.loc[inode_id]
    inode_type = inode['type']
    inode_name = inode['name']

    if inode_type == 'DIRECTORY':
        if inode_id in df_inode_dir.index:
            children = df_inode_dir.loc[inode_id]['children'].split(',')
            for child in children:
                child_inode = df_inode.loc[child]
                child_name = child_inode['name']
                child_type = child_inode['type']
                if child_type == 'DIRECTORY':
                    dir_queue.append(child)
                    curr_path_queue.append(current_path + '/' + child_name)
                else:
                    analyze_file(child_inode, temp_dic, current_path)

        if current_path in temp_dic:
            dir_summary = temp_dic[current_path]
            dir_files = dir_summary['files']
            if 'm_hot' in dir_summary:
                dir_summary['%m_hot'] = format(dir_summary['m_hot']/dir_files, '.2f')
            if 'm_warm' in dir_summary:
                dir_summary['%m_warm'] = format(dir_summary['m_warm']/dir_files, '.2f')
            if 'm_cold' in dir_summary:
                dir_summary['%m_cold'] = format(dir_summary['m_cold']/dir_files, '.2f')
            if 'a_hot' in dir_summary:
                dir_summary['%a_hot'] = format(dir_summary['a_hot']/dir_files, '.2f')
            if 'a_warm' in dir_summary:
                dir_summary['%a_warm'] = format(dir_summary['a_warm']/dir_files, '.2f')
            if 'a_cold' in dir_summary:
                dir_summary['%a_cold'] = format(dir_summary['a_cold']/dir_files, '.2f')
        else:
            dir_summary = ''
        print("|-{}\t{}".format(current_path, dir_summary))

    else:
        analyze_file(inode, temp_dic, current_path)

    analyze_inode_bfs(dir_queue, curr_path_queue, df_inode, df_inode_dir, temp_dic)


def analyze_file(file_inode, temp_dic, current_path):
    utc_now = int(time.time()*1000.0)
    child_mtime = int(file_inode['mtime'])
    child_atime = int(file_inode['atime'])

    if current_path in temp_dic:
        dir_temp = temp_dic[current_path]
    else:
        dir_temp = {'files': 0, 'bytes': 0}
        temp_dic[current_path] = dir_temp

    dir_temp["files"] += 1
    dir_temp["bytes"] += file_inode['numBytes']

    file_mtemp = utc_now - child_mtime
    if file_mtemp < HOT:
        dir_temp["m_hot"] = dir_temp["m_hot"] + 1 if "m_hot" in dir_temp else 1
    elif file_mtemp < WARM:
        dir_temp["m_warm"] = dir_temp["m_warm"] + 1 if "m_warm" in dir_temp else 1
    else:
        dir_temp["m_cold"] = dir_temp["m_cold"] + 1 if "m_cold" in dir_temp else 1

    file_atemp = utc_now - child_atime
    if file_atemp < HOT:
        dir_temp["a_hot"] = dir_temp["a_hot"] + 1 if "a_hot" in dir_temp else 1
    elif file_atemp < WARM:
        dir_temp["a_warm"] = dir_temp["a_warm"] + 1 if "a_warm" in dir_temp else 1
    else:
        dir_temp["a_cold"] = dir_temp["a_cold"] + 1 if "a_cold" in dir_temp else 1

def calculate_report(temp_dic):
    files = m_hots = m_warms = m_colds = bytes = 0
    a_hots = a_warms = a_colds = 0
    for dir_temp in temp_dic.values():
        files += dir_temp['files']
        bytes += dir_temp['bytes']
        if 'm_hot' in dir_temp: m_hots += dir_temp['m_hot']
        if 'm_warm' in dir_temp: m_warms += dir_temp['m_warm']
        if 'm_cold' in dir_temp: m_colds += dir_temp['m_cold']
        if 'a_hot' in dir_temp: a_hots += dir_temp['a_hot']
        if 'a_warm' in dir_temp: a_warms += dir_temp['a_warm']
        if 'a_cold' in dir_temp: a_colds += dir_temp['a_cold']

    report = {
        'path': args.dfs_path,
        'files': files,
        'bytes': bytes,
        'bytes_GB': format(bytes/1024/1024/1024, '.2f'),
        'm_hot': m_hots,
        '%m_hot': format(m_hots/files, '.2f'),
        'm_warm': m_warms,
        '%m_warm': format(m_warms/files, '.2f'),
        'm_cold': m_colds,
        '%m_cold': format(m_colds/files, '.2f'),
        'a_hot': a_hots,
        '%a_hot': format(a_hots/files, '.2f'),
        'a_warm': a_warms,
        '%a_warm': format(a_warms/files, '.2f'),
        'a_cold': a_colds,
        '%a_cold': format(a_colds/files, '.2f')
    }
    return report

if __name__ == '__main__':
    args = parse_args()

    if args.dfs_path == '/':
        print('Analyzing root path is not supported.')
        sys.exit(-1)

    # Get input binary fsimage file.
    if args.fetch_image:
        # Fetch latest fsimage from hdfs
        print("Fetching fsimage from HDFS")
        subprocess.call(['hdfs', 'dfsadmin', '-fetchImage', args.fetch_dir])

        # Get fetched fsimage filename
        files = [file for file in os.listdir(args.fetch_dir) if (file.startswith('fsimage_'))]
        if len(files) == 0:
            print('Failed fetching fsimage from HFDS.')
            sys.exit(-1)
        files.sort(key=os.path.getmtime, reverse = True)
        image_file = files[0]
    else:
        image_file = args.image_file

    if args.image_xml != '':
        image_xml = args.image_xml
    else:
        # Convert binary fsimage to xml format
        image_filename = os.path.basename(image_file)
        image_xml = args.convert_dir + "/" + image_filename + ".xml"
        subprocess.call(['hdfs', 'oiv', '-i', image_file, '-o', image_xml, '-p', 'XML'])

    # Parse fsimage xml file
    root_id, df_inode, df_inode_dir = parse_fsimage(image_xml)

    # Build data temperature dictionary
    print("\nAnalyzing {} ...".format(args.dfs_path))
    dfs_paths = args.dfs_path.strip('/').split('/')
    temp_dic = {}
    analyze_temperature(root_id, df_inode, df_inode_dir, dfs_paths, temp_dic)

    # Calculate report
    print("\nCreating report")
    report = calculate_report(temp_dic)
    print(json.dumps(report, indent=4))
