# Travel Meta Recommendation

Motels.home is a European based, widely known travel company, selling online bookings for hotels, motels and other 
kinds of accommodation. A very important part of their business is dealing with meta partners.

In a nutshell, Motel.home wants to advertise it's hotels on several partners site. In order to do that Motels.home 
has to pay some money for a given motel to appear on the partner's site. This is called bidding.
To make this a bit easier, partners expose a file with the recommended bids for the motels, so Motels.home knows 
which is the recommended price to pay if they want their motel to be listed.

## Input types

There are three kinds of inputs, you can find samples in src/test/resources/input and real data [here](../dataset):

1. bids.txt:
  - contains the bids for the given hotels for a given time for a given country
  - header:   
   ["MotelID", "BidDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE"]
  - example:   
   [0000002,11-05-08-2016,0.92,1.68,0.81,0.68,1.59,,1.63,1.77,2.06,0.66,1.53,,0.32,0.88,0.83,1.01]   
   You can read it like this: for motel with id 0000002 from 11-05-08-2016 (HH-dd-MM-yyyy) the prices are the following 
   for a country where the advertisement will be listed. This is Losa=Location of sale. 
   Let's say we want to advertise our motel in Hungary. We have to pay 0.92 Dollars to be listed.

2. motels.txt
  - the list of available motels
  - header:   
   ["MotelID", "MotelName", "Country", "URL", "Comment"]
  - example:   
   [0000001,Olinda Windsor Inn,IN,http://www.motels.home/?partnerID=3cc6e91b-a2e0-4df4-b41c-766679c3fa28,Some comment about the hotel]   
   this file is used to enrich the main datafeed, bids.txt

3. exchange_rate.txt 
  - exchange rates from USD to EUR
  - header:   
   ["ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate"]
  - example:   
   [11-06-05-2016,Euro,EUR,0.803]   
   This means that from 11-06-05-2016 (HH-dd-MM-yyyy) the conversion rate from USD to EUR was 0.803.


## Tasks

Please find the prepared skeleton in the source directory. You only have to substitute the ???-s with proper logic. 
You can also find some example input/output files in the test resource directory, you can use those to validate your logic.

1. Erroneous records   
   Nothing is perfect neither is the bidding system. From time to time something goes wrong in the bidding file 
   generator in the partner's side, so it includes corrupted records in the stream.   
   A record is corrupted if the value in the third column is not a number but a text with this format: ERROR_(.*)   
   The task here is to filter out the corrupted records from the input and count how many occurred from the given type 
   in the given hour.   
   You may want to use BidError domain class. The output should be formatted as comma separated lines containing
   the date (with hour precision), the error message and the count of such messages in the given hour.
   See "expected\_core" under expected\_out for example.   

2. Exchange rates   
   As Motels.home is Europe based it is convenient to convert all types of currencies to EUR for the business analysts.
   In our example we have only USD so we will do only USD to EUR conversion. Here you have to read the currencies into 
   a map where the dates are the keys and the related conversion rates are the values. Use this data as mapping source 
   to be able to exchange the provided USD value to EUR on any given date/time.   

3. Dealing with bids   
   Now we can focus on the most important parts, the bids. In Task 1 you have read the original bids data. 
   The first part is to get rid of the erroneous records and keep only the conforming ones which are not prefixed 
   with ERROR_ strings.    
   In this campaign Motel.home is focusing only on three countries: US,CA,MX so you'll have to only work with those three 
   and also the records have to be transposed so one record will only contain price for one Losa.   
   .    
   Example:   
  - original record:   
   ["MotelID", "BidDate", "HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE"]  
   [0000002,11-05-08-2016,0.92,1.68,0.81,0.68,1.59,,1.63,1.77,2.06,0.66,1.53,,0.32,0.88,0.83,1.01]   
  - keep only the three important ones   
  0000002,11-05-08-2016,1.59,,1.77
  - transpose the record and include the related Losa in a separate column   
  0000002,11-05-08-2016,US,1.59   
  0000002,11-05-08-2016,MX,   
  0000002,11-05-08-2016,CA,1.77   
  (This is closely related to SQL explode functionality)
   .   
   You may want to use BidItem domain class to represent the records.   
   .   
   Somewhere in this task you have to do the following conversions/filters:
  - Convert USD to EUR. The result should be rounded to 3 decimal precision.
  - Convert dates to proper format - use formats in Constants util class
  - Get rid of records where there is no price for a Losa or the price is not a proper decimal number

4. Load motels   
   Load motels data and prepare it for joining with bids.   
   Hint: we want to enrich the bids data with motel names, so you'll probably need the motel id and motel name as well.

5. Finally enrich the data and find the maximum   
   Motels.home wants to identify rich markets so it is interested where the advertisement is the most expensive 
   so we are looking for maximum values.   
   Join the bids with motel names. You can use EnrichedItem domain class to represent the join record.    
   As a final output we want to find and only keep the records which have the maximum prices for a given motelId/bidDate.
   When determining the maximum if the same price appears twice then keep the first object you found with the given price.
   .   
   Example:   
  - from records   
  0000001,Fantastic Hostel,2016-06-02 11:00,MX,1.50   
  0000001,Fantastic Hostel,2016-06-02 11:00,US,1.40   
  0000001,Fantastic Hostel,2016-06-02 11:00,CA,1.15   
  0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,MX,1.10   
  0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,US,1.20   
  0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,CA,1.30   
  - you will have to keep   
  0000001,Fantastic Hostel,2016-06-02 11:00,MX,1.50   
  0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,CA,1.30

## Testing

Some tests were provided to validate the implementation. The spark-testing-base library was used to determine RDD equality.

- Integration tests are responsible to validate that the whole flow is working correctly. They are checking only the
output files against the reference output for the provided sample data. We have two such tests, one for the error output
and one for the aggregated result.
- Unit level tests are also added but only a few. You are encouraged to extend the test suite to cover all the
implemented functionality

Please note that you are free to add any input/expected data based on your expectation of the behavior. You don't have to
add everything to input/output files. The examples are using those because of the size of the example input.

## Local environment

The following settings are suggestions for the local execution. Other configuration can work too but this is our proposal.

- To deal with files on windows you need winutils library:[winutils.zip](../env/winutils.zip)
- The used spark version should be 1.6 or 1.6.2
- Scala version should be 2.10
- Java version should be 1.8
- You will need to build and install (maven) the spark-parent and spark-parent-scala modules before the spark-core-homework-scala 
can be built. You can also add all 3 modules to your IDE for simplicity.
- Add the SPARK_HOME/lib directory as dependency to your spark-core-homework-scala project
- After implementing the unfinished methods you should be able to execute the MotelsHomeRecommendationTest which contains 
a few unit level tests and one system level test.


## Acceptance criteria

- missing parts are filled in
- only the core RDD functionalities are used (no DataFrame or DataSet)
- screenshot from local run
  - IntelliJ console - please include Spark logs as well (modify log4j.properties)
  - screenshot of each directory in local filesystem having the output files
- screenshots from HDP Sandbox
  - screenshot of each directory in HDFS having the output files
  - screenshot of SparkUI showing the various stages and tasks
  - duration of the job in seconds
  
Hint:   
- recommend you to look at [examples project](../examples)
- you can utilize cluster_submit.sh to launch the app in HDP Sandbox
