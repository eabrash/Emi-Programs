Emi-Programs
============

Code related to my programming blog (http://emiprograms.blogspot.com).
Please see individual pieces of code for reference and attributions. Much of this work is based on the U.W. Data Science Coursera course by Bill Howe (https://github.com/uwescience/datasci_course_materials).


HOW THE PIPELINE WORKS:


* Twitterstream.py collects messages from Twitter, within a geographical bounding box (~the S.F. Bay Area).

--> TwitterSample_AllBayArea_1408376915.18.txt is a sample output file from Twitterstream.py. It is just a small sample; a real analysis would use much more data.


* ExtractTweetData.py reads the raw data from the stream file and converts it into a tab-delimited file. This file does not contain all possible metadata, just the ones of interest for my particular project. A simple sentiment score based on the AFINN-111 matrix is also calculated for each Tweet.


* ParsedTweetReader.py takes the tab-delimited file and uses it to count incidences and average sentiment scores of query terms (provided as a .txt file) in each locale (city) represented in the data. The output is returned as a set of tables representing different statistics of interest for each query term in each city.

--> Parsed_Tweets13September2014_02.26PM_results_full.txt is a sample output file from ParsedTweetReader.py (the sample output of the whole pipeline). Note that it does not correspond to the sample input file listed above, but to a bigger dataset.



