#!/usr/bin/python3

#    Copyright (C) 2019  Dimitris Georgiou

#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.

#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.

#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.


import btrfs
import argparse
from functools import lru_cache
import math, array
import bisect,os
from collections import deque,Counter,OrderedDict

@lru_cache(maxsize=1024)
def unique_number(x,y):
    result=x
    if x >=y:
        result+=y
        result+=x**2
    else:
        result+=y**2
    return result

class Ranges:
    def __init__(self,start,stop,inode):
        self.inodes=[inode]
        self.starts=[start]
        self.stops=[stop]

    def add(self,start,stop,inode):
        self.inodes.append(inode)
        self.starts.append(start)
        self.stops.append(stop)

    def find_start_inodes(self,myrange):
        result=[]
        for i,item in enumerate(self.starts):
            if item == myrange:
                result.append(self.inodes[i])
        return result

    def find_stop_inodes(self,myrange):
        result=[]
        for i,item in enumerate(self.stops):
            if item == myrange:
                result.append(self.inodes[i])
        return result

    def convert_to_dictionary(self):
        #print self.starts,self.stops
        line= sorted(set(self.starts+self.stops))
        #print line

        data_dict=OrderedDict()

        for i, myrange in enumerate(line):
            #print i, myrange
            data_dict[myrange]=[]
            data_dict[myrange].extend(self.find_start_inodes(myrange))
            if i==0:
                continue
            else:
                data_dict[myrange].extend(data_dict[line[i-1]])
                for item in self.find_stop_inodes(myrange):
                    data_dict[myrange].remove(item)
        return data_dict


#Class to hold data. It's a dictionary of dictionaries.
#tree[key of the extent]= {range1: [list of inodes],range2: [list of inodes]}
#inodes data are used to find which files hold data of unique extents.
class TreeWrapper:
    def __init__(self):
        self._tree=dict()
        #self._inodes=dict()

    def add(self,key,start,stop,inode):
        if key in self._tree.keys():
            self._tree[key].add(start,stop,inode)
        else:
            self._tree[key]=Ranges(start,stop,inode)

    #this function analyzes the tree after all data are added.
    #for each range find which subvolumes use that range.
    #each snapshot has added its start and stop.
    #we keep the snapshots only in the start part.
    #scenario before: extent1:  pos_1[tree1]..........pos_2[tree2]....pos_3[tree2]...pos_4[tree1]
    #final result: pos_1[tree1]..........pos_2[tree1,tree2]....pos_3[tree1]...pos_4[]
    #the final range must become empty if additions were done correctly
    def transform(self):
        list_of_extents=list(self._tree.keys())
        for extent in list_of_extents:
            myranges=self._tree[extent]
            data_trans=myranges.convert_to_dictionary()
            self._tree[extent]=data_trans


    #return the sum of all data. It should be almost the same as the real data
    #used by the filesystem excluding metadata and without accounting raid level
    def __len__(self):
        result=0
        for extent,rangedict in self._tree.items():
            iterableview = list(rangedict.items())
            for i,mytuple in enumerate(iterableview):
                myrange,myset=mytuple
                #myset=list(myset)
                if len(myset)>=1:
                    try:
                        size=iterableview[i+1][0]-myrange
                        result+=size
                    except:
                        print(extent,rangedict.items(),mytuple)
        return result

    #find those ranges that have only one snapshot, if this snapshot is deleted
    #this space will be freed.
    #based on the scenario of transform is should return:
    #result[tree1]=pos2-pos1+pos4-pos3
    #result[tree2]=0
    #if files are analyzed use the inode data to find them ans store them in different dictionary.
    def find_unique(self):
        multi_segments=Counter()
        total_unique_data=0
        for extent,rangedict in self._tree.items():
            iterableview = list(rangedict.items())
            for i,range_inode_list in enumerate(iterableview):
                myrange,inode_list=range_inode_list
                #print(extent,myrange,inode_list)
                if len(inode_list)==1:
                    size=iterableview[i+1][0]-myrange
                    total_unique_data+=size
                if len(inode_list)>=1:
                    size=iterableview[i+1][0]-myrange
                    myset=set(inode_list)
                    for inode in myset:
                        multi_segments[inode]+=size
        return total_unique_data,multi_segments



def disk_parse(data_tree,path,tree):
          #print("Parsing subvolume:",tree)
          #pool = multiprocessing.Pool(processes=1)
          fs=btrfs.FileSystem(path)
          min_key=btrfs.ctree.Key(0,btrfs.ctree.EXTENT_DATA_KEY,0)
          for header, data in btrfs.ioctl.search_v2(fs.fd, tree,min_key):
            if header.type == btrfs.ctree.EXTENT_DATA_KEY:
              datum=btrfs.ctree.FileExtentItem(header,data)
              inode=datum.key.objectid
              if datum.type != btrfs.ctree.FILE_EXTENT_INLINE:# and datum.disk_bytenr !=0:
                      #file=btrfs.ioctl.ino_lookup(fs.fd,tree,inode)
                      key=unique_number(datum.disk_bytenr,datum.disk_num_bytes)
                      stop=datum.offset+datum.num_bytes
                      data_tree.add(key,datum.offset,stop,inode)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--root", type=int,default=5,
                    help="current active subvolume to analyze first, default is 5")
    parser.add_argument("path", type=str,
                    help="path of the btrfs filesystem")
    parser.add_argument("-o",'--output', nargs='?', type=argparse.FileType('w'),
                        help="File to write results for all files that are impacted be dedup")
    args=parser.parse_args()
    data_tree=TreeWrapper()
    #print("Out:",args.output)
    disk_parse(data_tree,args.path,args.root)    
    data_tree.transform()
    total_unique_data,uniquedatainode=data_tree.find_unique()
    total_data=len(data_tree)
    print("Disk space gained by dedup/reflink:",btrfs.utils.pretty_size(total_data-total_unique_data))
    print("Disk space used only by one file:",btrfs.utils.pretty_size(total_unique_data))
    print("Total disk space used by files:",btrfs.utils.pretty_size(total_data))
    print("Percentage gained by dedup {:.2%}".format((total_data-total_unique_data)/total_data))
    if args.output !=None:
        fs=btrfs.FileSystem(args.path)
        for inode,unique_size in uniquedatainode.items():
            file=None
            try:
                file=btrfs.ioctl.ino_lookup(fs.fd,args.root,inode)
            except:
                continue
            filename=args.path+"/"+file.name_bytes.decode('utf-8')
            filename=filename[:-1]
            #print(filename)
            full_size=os.path.getsize(filename)
            percentage=unique_size/full_size
            if percentage<1.0:
                line="{:>9} {:>9} {:>7.2%} {}\n".format(btrfs.utils.pretty_size(unique_size),btrfs.utils.pretty_size(full_size),percentage,filename)
                args.output.write(line)

if __name__ == '__main__':
    main()

