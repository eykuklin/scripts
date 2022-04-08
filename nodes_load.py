#!/usr/bin/python

# Parse collectl log files to acquire load of nodes (for the URAN cluster)
# Kuklin E.

from os.path import isfile, join, getsize
import os
#import sys
import time
import glob
import re

class nodeinfo:
    def __init__(self, cpu_load=0, mem_load=0, mem_amount=0, gpu_load=0, gpu_mem_load=0, gpu_mem_amount=0):
        self.cpu_load = cpu_load
        self.mem_load = mem_load
        self.mem_amount = mem_amount
        self.gpu_load = gpu_load
        self.gpu_mem_load = gpu_mem_load
        self.gpu_mem_amount = gpu_mem_amount
    def update(self, cpu_load, mem_amount, gpu_load, gpu_mem_amount):
        #Calc average cpu load and total memory amount
        self.cpu_load = int ((self.cpu_load + cpu_load)*0.5 )
        self.cpu_load = self.check_percent(self.cpu_load)
        self.mem_amount += mem_amount
        self.gpu_load = int ((self.gpu_load + gpu_load)*0.5 )
        self.gpu_load = self.check_percent(self.gpu_load)
        self.gpu_mem_amount += gpu_mem_amount
    def set_memload(self, mem_total, gpu_mem_total):
        #Calculate % mem load from total mem
        self.mem_load = int( (self.mem_amount * 100) / mem_total )
        self.mem_load = self.check_percent(self.mem_load)
        self.gpu_mem_load = int( (self.gpu_mem_amount * 100) / gpu_mem_total )
        self.gpu_mem_load = self.check_percent(self.gpu_mem_load)
    def check_percent(self, value):
        #Check if smth go wrong with % calculations
        if value > 100:
            value = 100
        if value < 0:
            value = 0
        return value
    def printinfo(self):
        output = ",".join([str(self.cpu_load),str(self.mem_load),str(self.mem_amount),str(self.gpu_load),str(self.gpu_mem_load),str(self.gpu_mem_amount)])
        return output

#Get nodes statistics
def get_info():

    #Node params
    #Tesla V-100 2x18 CPU
    mem_total_v100 = 395264
    gpu_mem_total_v100 = 262144
    #Apollo 1-32
    #395264 x 32 = 12648448
    mem_total_apollo = 12648448
    gpu_mem_total_apollo = 1
    #Tesla 1-51
    #Mem: 20 x 49152 + 10 x 196608 + 13 x 98304 + 5 x 65536 + 4 x 98304 = 4947968
    #GPU: 11441 x 3 x 6 + 6067 x 8 x 14 + 6067 x 6 = 205 938 + 679 504 + 36 402 = 921 844
    mem_total_tesla = 4947968
    gpu_mem_total_tesla = 921844

    #Create classes for partitions
    teslaV100 = nodeinfo()
    apollos = nodeinfo()
    teslas = nodeinfo()

    #Get a list of all files corresponding to current date
    yearmonth = time.strftime("%Y%m")
    os.chdir('/home2/collectl/')
    filelist = glob.glob('*' + yearmonth, recursive=False)
    time_now = time.time()        # fix the current time to check whether log files are outdated

    for filename in filelist:
        modify_time = os.path.getmtime(filename)
        if time_now - modify_time > 240:
            continue        #the file is too old (4 minutes unmodified), throw it away

        #Seek for the file end, read last line in bytes, and decode
        with open(filename, 'rb') as f:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
            line = f.readline().decode()
            line = line.rstrip('\n')
            fields = line.split(" ")

        if len(fields) < 8:
            continue
            #print("Bad file format: %s" % filename)
            #quit()

        (timeinfo, meminfo, netinfo, nfsinfo,
         cpuusrinfo, cpusysinfo, gpuinfo, gpumeminfo,
         wattsinfo, tempinfo) = fields[:10]

        #CPU load: cpuusrinfo & cpusysinfo first element is already the average value
        if cpuusrinfo:
            cpu_user_load = cpuusrinfo.split(",")
        else:
            cpu_user_load = '0'
        if cpusysinfo:
            cpu_sys_load = cpusysinfo.split(",")
        else:
            cpu_sys_load = '0'
        cpu_load = int(cpu_user_load[0]) + int(cpu_sys_load[0])

        #Memory
        if meminfo:
            mem_amount = int (int(meminfo) / 1024)
        else:
            mem_amount = 0


        #Checking start of filename: tesla-v100, apollo, tesla, and updating corresponding class
        match = re.match(r"apollo", filename)
        if match:
            apollos.update(cpu_load, mem_amount, 0, 0)
            

        match = re.match(r"tesla-v100", filename)
        if match:
            #GPU load
            if gpuinfo:
                gpu_load = gpuinfo.split(",")
                gpu_load = list(map(int, gpu_load))
                gpu_count = len(gpu_load)
                gpu_load = int (sum(gpu_load) / gpu_count)
            else:
                gpu_load = 0
            #GPU memory
            if gpumeminfo:
                gpu_mem_amount = gpumeminfo.split(",")              #string to list
                gpu_mem_amount = list(map(int, gpu_mem_amount))     #string list to int list
                gpu_mem_amount = sum(gpu_mem_amount)                #sum of elements
            else:
                gpu_mem_amount = 0
            teslaV100.update(cpu_load, mem_amount, gpu_load, gpu_mem_amount)
            
        match = re.match(r"tesla\d", filename)
        if match:
            match = re.match(r"tesla(47|48|49|50|51)", filename)
            if match:
                #GPU load
                #This nodes have only 3 GPU: gpu_load[0-2] - load, gpu_load[3-5] - memory
                if gpuinfo:
                    gpu_count = 3
                    gpu_load = gpuinfo.split(",")
                    gpu_load = list(map(int, gpu_load))
                    gpu_mem_amount = gpu_load[3:6]
                    gpu_load = gpu_load[:3]
                    gpu_load = int (sum(gpu_load) / gpu_count)
                    gpu_mem_amount = sum(gpu_mem_amount)
                else:
                    gpu_load = 0
                    gpu_mem_amount = 0
                teslas.update(cpu_load, mem_amount, gpu_load, gpu_mem_amount)
            else:
                #GPU load
                #Normal nodes with 8 GPU
                if gpuinfo:
                    gpu_load = gpuinfo.split(",")
                    gpu_load = list(map(int, gpu_load))
                    gpu_count = len(gpu_load)
                    gpu_load = int (sum(gpu_load) / gpu_count)
                else:
                    gpu_load = 0
                #GPU memory
                if gpumeminfo:
                    gpu_mem_amount = gpumeminfo.split(",")
                    gpu_mem_amount = list(map(int, gpu_mem_amount))
                    gpu_mem_amount = sum(gpu_mem_amount)
                else:
                    gpu_mem_amount = 0
                teslas.update(cpu_load, mem_amount, gpu_load, gpu_mem_amount)

    #Finally finished processing all files

    apollos.set_memload(mem_total_apollo, gpu_mem_total_apollo)
    teslaV100.set_memload(mem_total_v100, gpu_mem_total_v100)
    teslas.set_memload(mem_total_tesla, gpu_mem_total_tesla)

    print(teslaV100.printinfo() +','+ apollos.printinfo() +','+ teslas.printinfo())


def main():
    nodes_info = get_info()

if __name__ == "__main__":
    main()
