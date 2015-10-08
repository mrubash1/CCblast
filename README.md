 # [Origin] (http://mrubash1.com/origin)

 * ### Introduction
  * The [Commoncrawl] (http://commoncrawl.org/)raw data of ~1.8 billion web pages is stored on [S3](https://aws.amazon.com/s3/).The location of specific urls (i.e. *.wikipedia.org) are accessible via a queryable API, the [cdx-index-clinet] (https://github.com/ikreymer/pywb/wiki/CDX-Server-API#api-reference). Using the cdx-index-client, the location of the three file types per url, WARC, WET and WAT can be found.
    *WARC files which store the raw crawl data
    *WAT files which store computed metadata for the data stored in the WARC, including plaintext
    *WET files which store extracted plaintext from the data stored in the WARC 

  ![WARC file type](/img/warc.png) 

  * In order to download, parse and clean the commoncrawl data, the producing cluster of four m4 x xlarge run [CCBlast] (https://github.com/mrubash1/CCblast). CCBlast takes the S3 file location information from the [cdx-index-clinet] (https://github.com/ikreymer/pywb/wiki/CDX-Server-API#api-reference), downloads the relevant WAT and WET files to the local machines, parses the data to remove unnecessary metadata, cleans the data for problematic strings (i.e. 'true' instead of 'True'), then uploads the information to a personal S3 bucket. This personal S3 bucket and associated data will serve as the 'source of truth' for the Origin Platform.

 * ### Implementation
  *It is recommonded that you first use the [cdx-index-client] (https://github.com/ikreymer/cdx-index-client) to download your file locations of interest (as well as related metadata of those files), i.e: ./cdx-index-client.py -c CC-MAIN-2015-06 *.wikipedia.org --json --directory Wikipedia_CDX_index_results_January_2015 to download all subfolders of the January 2015 commoncrawl which contain '.wikipedia.org', download as a JSON file and store it in the indicated directory 

  *Next run the CC_Blast_Parser.py to isolate the commoncrawl locations you seek to access

  *Lastly, run CC_Blast.py with the correct domain extension (i.e. python CC_Blast.py 00 &). Depending on your available memory and cores, you can run multiple instances of CC_Blast with different extensions


