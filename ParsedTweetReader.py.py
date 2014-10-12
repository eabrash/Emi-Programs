import codecs
import numpy
import math

# This program takes a file of parsed tweets (output of ExtractTweetData.py),
# identifies the locales (cities) represented in the data, and, for each locale,
# counts the frequency and average message sentiment score for each term (word)
# included in an input list (e.g., "EducationTerms.txt").
#
# The output is given as a set of large tables, showing the statistics for each
# term of interest in each locale.
#
# How to fill an array with zeroes from: http://stackoverflow.com/questions/4056768/how-to-declare-array-of-zeros-in-python-or-an-array-of-a-certain-size
# Python readlines error with unicode and a workaround from: http://bugs.python.org/issue15278

extractedTweets = "<Name of output file from ExtractTweetData.py>"
queryTerms = "EducationTerms.txt"   # Input terms (words)
                                    # whose incidence you wish to calculate

## Calculates counts and basic statistics for the terms of interest included
## in the query, in the different locations indicated in the input list of
## locales.

def countQueryInstances (parsedTweetsFile, queryTermsArray, theLocalesArray):

    queryCounts = []
    queryScores = []
    numTweets = []
    scoreTweets = []
    indivTweets = []
    indivScores = []
    stdevArray = []
    stderrArray = []
    stdevOverall = []
    stderrOverall = []
    
    for i in range(0,len(theLocalesArray)):
        queryCounts.append([0] * len(queryTermsArray))     # Number of tweets where query term is found
        queryScores.append([0.0] * len(queryTermsArray))   # Composite score of tweets where query term is found
        indivScores.append([0] * len(queryTermsArray))
        stdevArray.append([0.0]*len(queryTermsArray))
        stderrArray.append([0.0]*len(queryTermsArray))
        numTweets.append(0)                              # Total number of tweets
        scoreTweets.append(0.0)                           # Composite score of all tweets
        indivTweets.append(0.0)
        stdevOverall.append(0.0)
        stderrOverall.append(0.0)

    for i in range(0,len(theLocalesArray)):
        indivTweets[i] = []
        for j in range(0,len(queryTermsArray)):
            indivScores[i][j] = []

    counter = 0
    
    for line in parsedTweetsFile:
        
        try:
            line = line.decode(encoding = "utf-8")
            lineArray = line.split("\t")            # Input file is preprocessed to remove
            
            if lineArray[11] in theLocalesArray:

                index = theLocalesArray.index(lineArray[11])
            
                messageList = lineArray[2].split()      #   any tabs or newlines in the messages
                
                alreadyFoundFlags = [False] * len(queryTermsArray)  # Flags to ensure that messages
                                                                    #   having the same term twice are not
                                                                    #   double-counted
                                                                    
                for i in range(0, len(messageList)):    # For each word in the message...
                    messageList[i] = messageList[i].strip(".!?-~@$#%^&*(){}[]\|:;,`+=_/")
                    ngram = messageList[i].encode("ascii","ignore")
                    ngram = ngram.lower()
                    
                    for j in range(0,len(queryTermsArray)): # Check each term in the query list to see if it matches the focal word
                        if alreadyFoundFlags[j] == False:   # If the query term has already been found in this message, don't check again
                            if ngram == queryTermsArray[j]:
                                queryCounts[index][j] += 1
                                queryScores[index][j] += int(lineArray[-1]) # Get the affect score of the message
                                alreadyFoundFlags[j] = True
                                indivScores[index][j].append(float(int(lineArray[-1])))            
                numTweets[index] += 1
                scoreTweets[index] += int(lineArray[-1])
                indivTweets[index].append(float(int(lineArray[-1])))
        except UnicodeEncodeError:
            print("UnicodeEncodeError")
            print(repr(line))
            print(lineArray[-1])
            print(str(len(lineArray)))
            break
        except ValueError:
            print("ValueError")
            print(repr(line))
            print(lineArray[-1])
            print(str(len(lineArray)))

    for i in range(0,len(theLocalesArray)):
        stdevOverall[i] = numpy.std(numpy.array(indivTweets[i]))
        stderrOverall[i] = stdevOverall[i]/math.sqrt(float(numTweets[i]))
        for j in range(0,len(queryTermsArray)):
            stdevArray[i][j] = numpy.std(numpy.array(indivScores[i][j]))
            stderrArray[i][j] = stdevArray[i][j]/math.sqrt(float(queryCounts[i][j]))
    
    return queryCounts, queryScores, numTweets, scoreTweets, stdevArray, stderrArray, stdevOverall, stderrOverall

## Identify all the locales in the input tweet file and make a list of them
## for subsequent use.

def populateLocales(tweetFileName):

    myLocalesArray = []
    tweetFile = open(tweetFileName, mode = "rU")
    
    for line in tweetFile:
        line = line.decode(encoding = "utf-8")
        lineArray = line.split("\t")

        if lineArray[10] == "city" and lineArray[12].split()[-1] == "CA":
            if lineArray[11] not in myLocalesArray:
                myLocalesArray.append(lineArray[11].encode("ascii","ignore"))

    tweetFile.close()

    return myLocalesArray
            
        
def main():

    queryArray = []

    queryFile = open(queryTerms, mode = "rU")

    # Read query terms into array
    
    for line in queryFile:
        queryArray.append(line.strip())

    queryFile.close()
    
    localesArray = populateLocales(extractedTweets)

    #localesArray = ["Oakland", "Palo Alto", "East Palo Alto", "San Francisco", "San Jose","Menlo Park"]

    # Open file of parsed tweets as utf-8. I've taken care to preserve the
    # utf-8 encoding so that emoticons and foreign characters can be recovered,
    # though the current implementation does not take advantage of this
    # (hopefully will in the future).

    tweetsFile = open(extractedTweets, mode = "rU")
  
    queryCounts, queryScores, totalTweets, totalScores, stdevs, stderrs, stdevO, stderrO = countQueryInstances(tweetsFile, queryArray, localesArray)

    overallAverageScores = []

    for i in range(0,len(localesArray)):

        if (totalTweets[i] > 0):
            overallAverageScores.append(totalScores[i]/float(totalTweets[i]))
        else:
            overallAverageScores.append("undef")
            print("Place with zero tweets: " + localesArray[i])

    # Write the output to a file in tabular format

    outFileName = extractedTweets[0:-4] + "_results_full.txt"

    outFile = open(outFileName, mode = "w")

    outFile.write("OVERALL\n")
    outFile.write("Metric\t")

    for i in range(0,len(localesArray)):
        outFile.write(localesArray[i] + "\t")

    outFile.write("\n")
    outFile.write("Total tweets\t")

    for i in range(0,len(localesArray)):
        outFile.write(str(totalTweets[i]) + "\t")    

    outFile.write("\n")
    outFile.write("Aggregate score\t")

    for i in range(0,len(localesArray)):
        outFile.write(str(totalScores[i]) + "\t")

    outFile.write("\n")
    outFile.write("Average score\t")

    for i in range(0,len(localesArray)):
        outFile.write(str(overallAverageScores[i]) + "\t")

    outFile.write("\n")
    outFile.write("Stdev\t")

    for i in range(0,len(localesArray)):
        outFile.write(str(stdevO[i]) + "\t")

    outFile.write("\n")
    outFile.write("Std err\t")

    for i in range(0,len(localesArray)):
        outFile.write(str(stderrO[i]) + "\t")

    outFile.write("\n\n")
    outFile.write("COUNT\n")
    outFile.write("Term\t")

    for i in range(0,len(localesArray)):
        outFile.write(localesArray[i] + "\t")

    outFile.write("\n")

    for i in range(0,len(queryArray)):
        outFile.write(queryArray[i] + "\t")
        for j in range(0,len(localesArray)):
            outFile.write(str(queryCounts[j][i]) + "\t")
        outFile.write("\n")

    outFile.write("\n\n")
    outFile.write("FREQUENCY\n")
    outFile.write("Term\t")

    for i in range(0,len(localesArray)):
        outFile.write(localesArray[i] + "\t")

    outFile.write("\n")

    for i in range(0,len(queryArray)):
        outFile.write(queryArray[i] + "\t")
        for j in range(0,len(localesArray)):
            if (totalTweets[j] > 0):
                outFile.write(str(float(queryCounts[j][i])/float(totalTweets[j])) + "\t")
            else:
                outFile.write("undef\t")                
        outFile.write("\n")

    outFile.write("\n\n")
    outFile.write("AGGREGATE SCORE\n")
    outFile.write("Term\t")

    for i in range(0,len(localesArray)):
        outFile.write(localesArray[i] + "\t")

    outFile.write("\n")

    for i in range(0,len(queryArray)):
        outFile.write(queryArray[i] + "\t")
        for j in range(0,len(localesArray)):
            outFile.write(str(queryScores[j][i]) + "\t")
        outFile.write("\n")

    outFile.write("\n\n")
    outFile.write("AVERAGE SCORE\n")
    outFile.write("Term\t")

    # --> START HERE, NEED TO CALCULATE AVERAGES

    averagesArray = []
    normsArray = []

    for i in range(0,len(localesArray)):
        averagesArray.append([0.0] * len(queryArray))
        normsArray.append([0.0] * len(queryArray))

    for i in range(0,len(localesArray)):
        for j in range(0,len(queryArray)):
            if queryCounts[i][j] > 0:
                averagesArray[i][j]=str(queryScores[i][j]/float(queryCounts[i][j]))
                if overallAverageScores[i] > 0:
                    normsArray[i][j]=str((queryScores[i][j]/float(queryCounts[i][j]))/(overallAverageScores[i]))
                else:
                    normsArray[i][j]="undef"
            else:
                averagesArray[i][j]="undef"
                normsArray[i][j]="undef"

    for i in range(0,len(localesArray)):
        outFile.write(localesArray[i] + "\t")

    outFile.write("\n")

    for i in range(0,len(queryArray)):
        outFile.write(queryArray[i] + "\t")
        for j in range(0,len(localesArray)):
            outFile.write(averagesArray[j][i] + "\t")
        outFile.write("\n")

    outFile.write("\n\n")
    outFile.write("STANDARD DEVIATION OF SCORES\n")
    outFile.write("Term\t")

    for i in range(0,len(localesArray)):
        outFile.write(localesArray[i] + "\t")

    outFile.write("\n")

    for i in range(0,len(queryArray)):
        outFile.write(queryArray[i] + "\t")
        for j in range(0,len(localesArray)):
            outFile.write(str(stdevs[j][i]) + "\t")
        outFile.write("\n")

    outFile.write("\n\n")
    outFile.write("STANDARD ERROR OF SCORES\n")
    outFile.write("Term\t")

    for i in range(0,len(localesArray)):
        outFile.write(localesArray[i] + "\t")

    outFile.write("\n")

    for i in range(0,len(queryArray)):
        outFile.write(queryArray[i] + "\t")
        for j in range(0,len(localesArray)):
            outFile.write(str(stderrs[j][i]) + "\t")
        outFile.write("\n")

    outFile.write("\n\n")
    outFile.write("NORMALIZED SCORE\n")
    outFile.write("Term\t")

    for i in range(0,len(localesArray)):
        outFile.write(localesArray[i] + "\t")

    outFile.write("\n")

    for i in range(0,len(queryArray)):
        outFile.write(queryArray[i] + "\t")
        for j in range(0,len(localesArray)):
            outFile.write(normsArray[j][i] + "\t")
        outFile.write("\n")
                
    outFile.close()
    tweetsFile.close()

if __name__ == '__main__':
    main()
