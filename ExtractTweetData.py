## This program reads raw streamed data (JSON format) from Twitter and converts
## it into a tab-delimited file containing the message text (UTF-8 preserved),
## some metadata, and a sentiment score. The sentiment score is based on the
## AFINN-111 file and calculated according to a very simple metric described
## in the UW Data Science Coursera course (see the course GitHub repository
## for more information): https://github.com/uwescience/datasci_course_materials/tree/master/assignment1.
##
## Code to get date/time in readable format is from: http://stackoverflow.com/questions/13890935/timestamp-python

import sys
import json
import datetime
import codecs
import time

sentimentFileInput = "AFINN-111.txt"

# Put all of your desired Twitterstream input files into this list, separated
# by commas
tweetFilesInput = ["TwitterSample_AllBayArea_1408376915.18.txt"]

aDict = {}
global numTweets
numTweets = 0
global failedLines
failedLines = 0

# Make a usable AFINN dictionary from the AFINN source file
def populateAFINNDictionary(file):
    #counter = 0
    afinnDict = {}
    for line in file:
        myLine = line.split("\t")
        afinnDict[myLine[0]] = int(myLine[1])
        #if " " in myLine[0]:
        #    print(myLine[0])
        #counter += 1
    return afinnDict

# Calculate the score for a message based on the AFINN dictionary and message text
# (considers only n-grams of one word)
def calculateMessageScore(message, afinn):
    messageList = message.split()
    messageScore = 0
    
    for i in range(0, len(messageList)):
        messageList[i] = messageList[i].strip(".!?-~@$#%^&*(){}[]\|:;,`+=_/")
        ngram = messageList[i].encode("ascii","ignore")
        ngram = ngram.lower()
        if ngram in afinn.keys():
            #print(ngram)
            messageScore += afinn[ngram]
    return messageScore
            
# Takes a file of tweets as input, goes through to find the fields in which we are interested, and writes these to an
# output file in a column format.
def parseTweets(inFile, outFile, afinnDict, errorFile):
    for line in inFile:
        if line:
            try:    
                jsonLine = json.loads(line)
                if u'text' in jsonLine.keys() and u'id_str' in jsonLine.keys():     ## Avoids getting deletes along with posts
                    # Need to create a conditonal to handle the case where these values are null

                    if jsonLine[u'created_at']:
                        created_at = jsonLine[u'created_at']
                    else:
                        created_at = ""

                    if jsonLine[u'id_str']:
                        id_str = jsonLine[u'id_str']
                    else:
                        id_str = ""
                        
                    if jsonLine[u'text']:
                        text = jsonLine[u'text'].replace("\n"," ").replace("\r"," ").replace("\t"," ") # Replace statements are to prevent line read and parsing errors when data is read back in later
                    else:
                        text = ""

                    # Check that user field is not null before attempting to access values

                    if jsonLine[u'user']:

                        if jsonLine[u'user'][u'id_str']:
                            user_id_str = jsonLine[u'user'][u'id_str']
                        else:
                            user_id_str = ""

                        if jsonLine[u'user'][u'name']:
                            user_name = jsonLine[u'user'][u'name'].replace("\n"," ").replace("\r"," ").replace("\t"," ")
                        else:
                            user_name = ""

                        if jsonLine[u'user'][u'screen_name']:
                            user_screen_name = jsonLine[u'user'][u'screen_name'].replace("\n"," ").replace("\r"," ").replace("\t"," ")
                        else:
                            user_screen_name = ""

                        if jsonLine[u'user'][u'location']:
                            user_location = jsonLine[u'user'][u'location'].replace("\n"," ").replace("\r"," ").replace("\t"," ")
                        else:
                            user_location = ""

                    else:

                        user_id_str = ""
                        user_name = ""
                        user_screen_name = ""
                        user_location = ""

                    # Must ensure coordinates field is not null before attempting to access values

                    if jsonLine[u'coordinates']:
                        
                        if jsonLine[u'coordinates'][u'type']:
                            coordinates_type = jsonLine[u'coordinates'][u'type']
                        else:
                            coordinates_type = ""
                            
                        if jsonLine[u'coordinates'][u'coordinates']:
                            coordinates_coordinates = jsonLine[u'coordinates'][u'coordinates']
                        else:
                            coordinates_coordinates = ""
                            
                    else:
                        
                        coordinates_type = ""
                        coordinates_coordinates = ""


                    # Ensure place field is not null before attempting to access values

                    if jsonLine[u'place']:

                        if jsonLine[u'place'][u'id']:
                            place_id = jsonLine[u'place'][u'id']
                        else:
                            place_id = ""

                        if jsonLine[u'place'][u'place_type']:
                            place_place_type = jsonLine[u'place'][u'place_type']
                        else:
                            place_place_type = ""

                        if jsonLine[u'place'][u'name']:
                            place_name = jsonLine[u'place'][u'name'].replace("\n"," ").replace("\r"," ").replace("\t"," ")
                        else:
                            place_name = ""

                        if jsonLine[u'place'][u'full_name']:
                            place_full_name = jsonLine[u'place'][u'full_name'].replace("\n"," ").replace("\r"," ").replace("\t"," ")
                        else:
                            place_full_name = ""

                        if jsonLine[u'place'][u'bounding_box']:
                            
                            if jsonLine[u'place'][u'bounding_box'][u'type']:
                                place_bounding_box_type = jsonLine[u'place'][u'bounding_box'][u'type']
                            else:
                                place_bounding_box_type = ""

                            if jsonLine[u'place'][u'bounding_box'][u'coordinates']:
                                place_bounding_box_coordinates = jsonLine[u'place'][u'bounding_box'][u'coordinates']
                            else:
                                place_bounding_box_coordinates = ""
                        else:
                            place_bounding_box_type = ""
                            place_bounding_box_coordinates = ""
                    else:
                        
                        place_id = ""
                        place_place_type = ""
                        place_name = ""
                        place_full_name = ""
                        place_bounding_box_type = ""
                        place_bounding_box_coordinates = ""

                    if jsonLine[u'lang']:
                        lang = jsonLine[u'lang']
                    else:
                        lang = ""
                        
                    score = calculateMessageScore(text, afinnDict)

                    outFile.write(created_at + "\t" + id_str + "\t" + text + "\t" + user_id_str + "\t" + user_name + "\t" +
                                  user_screen_name + "\t" + user_location + "\t" + coordinates_type + "\t" + str(coordinates_coordinates) + "\t" + place_id + "\t" +
                                  place_place_type + "\t" + place_name + "\t" + place_full_name + "\t" + place_bounding_box_type + "\t" +
                                  str(place_bounding_box_coordinates) + "\t" + lang + "\t" + str(score)+ "\r")
            
                    global numTweets
                    numTweets += 1
                    
            except:
                
                global failedLines
                failedLines += 1
                if line.encode("ascii","ignore") != "":
                    errorFile.write(line)
def main():

    global numTweets
    global failedLines

    startTime = datetime.datetime.now().strftime("%d%B%Y_%I.%M%p")
    
    sentimentFile = open(sentimentFileInput, mode = "rU")
    aDict = populateAFINNDictionary(sentimentFile)

    parsedTweetsName = "Parsed_Tweets" + datetime.datetime.now().strftime("%d%B%Y_%I.%M%p")
    
    parsedTweetFile = codecs.open(parsedTweetsName + ".txt", mode = "w", encoding = "utf8")
    parsedTweetLogFile = open(parsedTweetsName + "_log.txt", mode = "w")        # Log file for processing
    parsedTweetErrorFile = open(parsedTweetsName + "_failed.txt", mode = "w")   # File containing lines that could not be parsed

    for currentFile in tweetFilesInput:
        tweetFile = open(currentFile, mode = "rU")
        parseTweets(tweetFile, parsedTweetFile, aDict, parsedTweetErrorFile)
        tweetFile.close()

    endTime = datetime.datetime.now().strftime("%d%B%Y_%I.%M%p")

    parsedTweetLogFile.write("Start time: " + startTime + "\nEnd time: " + endTime + "\nTweets Successfully Processed: " + str(numTweets) + "\nFailed Lines: " + str(failedLines))
    parsedTweetFile.close()
    parsedTweetErrorFile.close()

if __name__ == '__main__':
    main()

