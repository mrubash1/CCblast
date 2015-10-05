'''
Created on Sep 15, 2015
@author: MatthewRubashkin
'''

import simplejson as json
import os

#Global Variables
key = "filename" #filename=WARC directory
filename = 'domain-ncbi.nlm.nih.gov-'
rootdir = 'PubMed_CDX_index_results_January_2015'

#Function to get out key value defined in global variable
def json_get_key_value(json_string,key):
    json_object = json.loads(json_string)
    json_parsed = json_object[key]
    return json_parsed
    #print 'KEY VALUE OBTAINED'
    #print


    
#Write parsed json value from file, specified by filename
def batch_WARC_file_destination(filename,rootdir):
    processed_directory = rootdir + '_processed'
    with open(processed_directory+'/'+filename, 'w+') as file: #file to write
        with open(rootdir+'/'+filename, 'r') as f: #file to read
            #ry:
                json_files = f.readlines()
                for line in json_files:
                    #Get WARC file
                    #print 'LINE: ', line
                    line = str(line)
                    ##EDIT LATER##
                    # THIS IN HERE TO CORRECT AN ERROR IN THE CDX CLIENT 150923
                    start_index=(line.index('{'))
                    line = line[start_index:] #Start read at url only
                    #print 'NEW LINE: ', line
                    ##EDIT LATER##
                    json_parsed=json_get_key_value(line,key)
                    file.write(str(json_parsed)+'\n')
                    #print 'FILE WRITTEN WITH ITERATOR'
                    #print
            #except:
                #pass

def Iterator_for_WET_and_WAT_files(filename, rootdir):
    processed_directory = rootdir + '_processed'
    if not os.path.exists(processed_directory):
        os.makedirs(processed_directory)
    for dirs, subdirs, files in os.walk(rootdir):
        for file in files:
            if file != '.DS_Store':
                batch_WARC_file_destination(str(file),rootdir)
            print 'This file is complete: ', file

print 'running...'
Iterator_for_WET_and_WAT_files(filename, rootdir)

print 'end'
