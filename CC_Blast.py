# Matthew Rubashkin 150916
# Input to Kafka from S3 Common Crawl Server

# tools to open WARC files from common crawl
import warc, boto, json, ast, re, os,sys
from gzipstream import GzipStreamFile
from itertools import *
from boto.s3.key import Key
from boto.s3.connection import S3Connection

#create WARC file from s3 WARC like file
def create_WARC_file_from_Common_Crawl(file_name, pds):
  k=Key(pds)
  k.key=file_name
  return warc.WARCFile(fileobj=GzipStreamFile(k))

#Using URL descriptor, get text information form WARC file
def get_text_from_WET_file(WET_file_name, WARC, descriptor, output_directory):
  list_of_json_files=[]
  file_name=WET_file_name.replace('/','_')+'.json'
  #create file to write data
  saving_directory = output_directory + '/' + file_name
  #print 'SAVING DIRECTORY: ', saving_directory
  file = open(saving_directory, "w+") #instantiate file first to avoid r+ not making file
  file.close()
  file = open(saving_directory, "r+") #open for read and write
  #print 'This is the "file" string: ', file.name
  #process plaintext in WET file
  for num, record in enumerate(WARC):
    url = record.header.get('warc-target-uri', None)
    if descriptor in str(url):
      text = record.payload.read()
      try: #see if the plaintext can be formatted
        plaintext = re.sub(r'<.*?>', '', text) #remove random characters to avoid JSON errors
        plaintext=plaintext.decode('unicode_escape').encode('ascii','ignore') #remove unicode characters to avoid JSON errors
      except:
        #print 'CANNOT FORMAT CORRECTLY'
        pass
      #plaintext=plaintext.replace(('\n', '')) #remove all new lines to avoid JSON errors
      #print 'PLAINTEXT: ', plaintext
      data = {}
      data['url'] = str(url)
      data['plaintext'] = str(plaintext)
      WET_json_data = json.dumps(data)
      #print WET_json_data
      #print
      list_of_json_files.append(WET_json_data)
      if type(WET_json_data) == str:
        file.write(WET_json_data + '\n') # WRITE OUT TO FILE
        #print 'WET file processed: ', file_name, ' - with URL: ', url
        #print
  #print 'This is the "file" string: ', str(file)
  if download_to_s3 == True:
    run_the_s3_download(file) #RUN THE S3 DOWNLOAD FUNCTION!
  #print 'Uploaded WET text information to s3'
  file.close()
  return list_of_json_files

#Using URL descriptor, get metadata information form WARC file
def process_WAT_file(WAT_file_name, WARC, descriptor,output_directory):
    list_of_json_files=[]
    #Create file to write data
    file_name=WAT_file_name.replace('/','_')+'.json'
    #open the file and make sure it can be created first
    file = open(output_directory + '/' + file_name, "w+")
    file.close()
    file = open(output_directory + '/' + file_name, "r+")
    #print 'This is the "file" string: ', file.name
    #Process WAT file
    for num, record in enumerate(WARC): #this is where there are problems
        url = record.header.get('warc-target-uri', None)
        if descriptor in str(url):
            plaintext = record.payload.read()
            WAT_json_data=get_links_from_Messy_WAT_file_plaintext(url, plaintext) # run function
            #print WAT_json_data
            #print type(WAT_json_data)
            list_of_json_files.append(WAT_json_data)
            if type(WAT_json_data) == str:
                file.write(WAT_json_data + '\n') # WRITE OUT TO FILE
                #print 'WAT file processed: ', file_name, ' - with URL: ', url
                #print
    if download_to_s3 == True:
        run_the_s3_download(file) #RUN THE S3 DOWNLOAD FUNCTION!
    #print 'Uploaded WAT information to s3: '
    file.close()
    return list_of_json_files

def get_links_from_Messy_WAT_file_plaintext(url, plaintext):
    #Convert text to a dict type, making sure to capitalize true and false to avoid error
    plaintext_dict = ast.literal_eval((plaintext.replace('true','True')).replace('false','False')) #update later to one faster pass
    #print plaintext_dict
    #Variables for creating end json file
    #url_base='http://en.wikipedia.org'
    #url_base='http://www.ncbi.nlm.nih.gov'
    url_base='http://www.etsy.com'
    analyze_wikipedia_only= True
    list_of_json_files = []
    data = {}
    data['url'] = str(url) #instantiate dict for data
    link_list = []
    try: #make sure that the WAT file acutally has links
        links = plaintext_dict["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
        #base link to add - will only analyze wikipedia
        #data['links'] = link_list
        for link in links:
            link= link.values()[link.keys().index('url')]
            link_is_valid=False #instantiate first as false then modify if true
            #if link[0:5] in '/wiki': #check that it is a hyperlink from wikipedia
            if link[0] in '/': #check that it is a hyperlink from wikipedia
                full_link=url_base+link
                #print full_link
                link_list.append(full_link)
                link_is_valid=True
            if link_is_valid == True:
                link_list.append(full_link)
                #print link_list
        #Construct json_data that includes URL and link
        data['links'] = link_list
        WAT_json_data = json.dumps(data)
        #print list_of_json_files
        #print 'These are the links for: ', url
        return WAT_json_data
    except:
        pass
        #print 'There are no links for', url 
    #print #blank

#Function to make a WAT location file fram a WARC location file
def json_make_WAT_from_WARC(json_string):
    json_string = json_string.replace('/warc/','/wat/')
    json_string = json_string.replace('warc.gz','warc.wat.gz')
    return json_string

#Function to make a WET location file fram a WARC location file
def json_make_WET_from_WARC(json_string):
    json_string = json_string.replace('/warc/','/wet/')
    json_string = json_string.replace('warc.gz','warc.wet.gz')
    return json_string

def upload_to_s3(aws_access_key_id, aws_secret_access_key, file, bucket, key, callback=None, md5=None, reduced_redundancy=False, content_type=None):
    """
    Uploads the given file to the AWS S3
    bucket and key specified.
    callback is a function of the form:
    def callback(complete, total)
    The callback should accept two integer parameters,
    the first representing the number of bytes that
    have been successfully transmitted to S3 and the
    second representing the size of the to be transmitted
    object.
    Returns boolean indicating success/failure of upload.
    """
    try:
        size = os.fstat(file.fileno()).st_size
    except:
        # Not all file objects implement fileno(),
        # so we fall back on this
        file.seek(0, os.SEEK_END)
        size = file.tell()

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = conn.get_bucket(bucket, validate=True)
    k = Key(bucket)
    k.key = key
    if content_type:
        k.set_metadata('Content-Type', content_type)
    sent = k.set_contents_from_file(file, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)

    # Rewind for later use
    file.seek(0)

    if sent == size:
        return True
    return False

def run_the_s3_download(file):
    key = file.name
    bucket = 'rubash-commoncrawl'
    if upload_to_s3(aws_access_key, aws_secret_access_key , file, bucket, key):
        Upload_Successful= True
        #print 'Upload to S3 was successfulIt worked!'
    else:
        Upload_Successful= True
        #print 'The upload failed...'

#Run through all files in rootdir folder to grab information from S3
def Iterator_for_opening_files(rootdir, input_directory, output_directory):
    total_records_counter=0
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    #file_name = 'domain-wikipedia.org-' + var #updated to open a specific file using the global input var
    #file_name = 'domain-ncbi.nlm.nih.gov-' + var 
    file_name = 'domain-etsy.com-' + var
    #print 'THIS IS THE FILE NAME: ', file_name
    with open(input_directory+'/'+file_name) as f: #file to read
        #CREATE LOG FILE FOR STORING WHICH DID NOT OPEN
        log_directory = output_directory + '/' + file_name + '_log'
        #print 'SAVING DIRECTORY: ', saving_directory
        file = open(log_directory, "w+") 
        file.close()
        try:
            filename_lines = f.readlines()
            #print 'THESE ARE THE LINES'
            for line in filename_lines:
                try:
                    base_name = str(line).replace('\n', '') #replace line space
                    #convert .warc to .wet and .wat, with proper string replacements
                    filename_WET=json_make_WET_from_WARC(base_name)
                    filename_WAT=json_make_WAT_from_WARC(base_name)
                    #print 'LOOK AT THIS: ', filename_WET
                    #Uncompress the WET and WAT files with the proper S3 key
                    WARC_WET=create_WARC_file_from_Common_Crawl(filename_WET,pds)
                    WARC_WAT=create_WARC_file_from_Common_Crawl(filename_WAT,pds)
                    #get the plaintext and link information
                    get_text_from_WET_file(filename_WET, WARC_WET,descriptor,output_directory)
                    process_WAT_file(filename_WAT,WARC_WAT,descriptor,output_directory)
                    #print 'This file is complete: ', file
                    total_records_counter=total_records_counter+1
                    #print 'Total set of files processed: ', total_records_counter
                    #print
                except:
                    #just pass if an individual line could not be opened from S3
                    pass
        except:
            saving_directory = output_directory + '/' + file_name
            #print 'SAVING DIRECTORY: ', saving_directory
            file = open(log_directory, "w") #instantiate file first to avoid r+ not making file
            file.write(file_name + ' NOTuploaded' + '\n')
            file.close()
            print 'THERE WAS AN EXCEPTION IN OPENING AWS FILE: ', 
            pass

        #record what happened in log
        file = open(log_directory, "w")
        file.write(file_name + ' uploaded' + '\n')
        file.close()


def main():
    #RUN EVERYTHING
    Iterator_for_opening_files(rootdir, input_directory, output_directory) #Use the input here to launch

#Run WAT
#WARC_WAT=create_WARC_file_from_Common_Crawl(WAT_file_name,pds)
#process_WAT_file(WAT_file_name,WARC_WAT,descriptor)
#Run WET
#WARC_WET=create_WARC_file_from_Common_Crawl(WET_file_name,pds)
#get_text_from_WET_file(WET_file_name, WARC_WET,descriptor)

#if __name__ == "__main__":
#    var = str(raw_input("Please enter file NUMBER to open: "))
#    print "you entered", str(var)
#    main(var)

if __name__ == "__main__":
    #GET variable from the command line system argument
    var = str(sys.argv[1])
    #print "you entered file", str(var)
    #var = '000'

    #Variables to keep global
    #Connect to amazon S3 containing commoncrawl, no authenitcation necessary
    conn = boto.connect_s3(anon=True, debug=2)
    pds = conn.get_bucket('aws-publicdatasets')

    #Insert file names WET: plain text; WAT: hyperlinks
    rootdir = 'Etsy_CDX_index_results_January_2015'
    input_directory = rootdir + '_processed'
    output_directory = rootdir + '_WHOLE_DATA_SET_151004'

    #Connect to Personal S3 for downloading the data
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

    #Descriptor: limit the urls downloaded to this inclusion filter
    #descriptor = "en.wikipedia"
    descriptor = 'etsy.com'

    #Determines whether to download to S3 or just run files to see output
    download_to_s3 = True
    
    #now run everything
    main()
