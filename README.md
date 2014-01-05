Scraper code for TFL website

TODO
----

1. add scrapers for other feeds
1. finish refactor
1. need a good setup for hdf5 file...should add some meta data, compress, index etc
1. start doing summaries of trains and look for patterns
1. lots of general data exploration, need to understand the dataset
1. Ways to view, plot, graph, visualize the data. (see below)
1. Start thinking of predicting problems?
1. add tfl docs and write a doc of my own describing data
1. set up script for deployment to server (may try out virtualization and docker for fun ;-)
1. more robust collector
1. monitoring and stats -- scraper should dump size and # of files into rrd on every run. Parser should do the same for records. Metadata on storage locations too.
1. Monitor size of production hdf5 file

NOTES
-----

So far I am able to collect the detailed and summary predictions. Everything is going into S3 and a local dir right now. There is the slight concern regarding data size as we are collecting. I should end up collecting at the rate of >100GB/year ($10/month). Thought there is a chance that I can glacier most of that after analysis ($1/month). The xml flattening stuff is very easy but relatively time consuming. The number of rows get enormous quickly and there is a ton of redundant info. I have to start coming up with ideas to shrink the data set into useable bits. The useable stuff should get stuck somewhere more databas-y. What is suitable? Mongo, HDF5, ???

The goal for the secondary storage is to find something that is compact and cheap but can still be properly queried. We first have to define how the filtered and compacted records look like then we have to store them. Mongo is quite expensive for this, HDF5 is quite restrictive. Perhaps one of the newer ones(rethinkDB for example)

Next steps are to increase the information ratio of records. Currently we can derive current position of train, time to each station on the line from each record. So every 30 seconds we know how long to each station and how far the train has moved. We would like to find out how fast it takes a train to move between stations (sections of track). We should also be able to use the time to each station to look ahead to see trouble ahead of that train. We would also like to find out how long trains stay in the station and possibly if trains are congested or not. Being able to recognize congestion, minor delays, major delays is important. I think I should be able to get a lot of info out of the track section field if it increases linearly along the line. Technically that could be related to a distance.

Would like to get the track sections for each station. That could be useful.

We also need to store intermediate results and stats in a db. This will be used to run the website. Here it makes more sense to use a mongodb type system as that is mostly unstructured text that will be shown repeatedly.

Would like to build a few standard visuals and batch analysis jobs. Should plan on making as much stuff reusable so that things can be transformed easily into web stuff.

Plan for a webpage and mobile apps?

ARGH! Lots to do!


THOUGHTS
--------

* Is track id linear?
* how is velocity/time between stations most accurately defined?
* how do I remove the variability caused by the 30 second sample frequency?
* meaningful metrics for train speed/times/disruptions etc
* correlate track closures to train speeds using other feed
