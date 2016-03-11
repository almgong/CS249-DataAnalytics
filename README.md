#Class project for CS249
#Please follow the below directions to run our project implementation. To speed
up runtime of our code, we have provided files necessary on DropBox. You can 
choose to either (1.) run the code from scratch (may take ~ 2 hours and requres
no downloading from our DropBox links) OR (2.) you can follow the alternative 
method (labeled (2.)) and download and place the required output files to only 
run logistic regression and get the evaluation results much faster

Some requirements:
~10gb of free hard disk space (don't worry, our code deletes our temporary files).
Python must be installed.
The numpy python module must be installed.
Access to a command line is necesary.


############# 
## 1. From beginning to end
#############

1. Download/build Apache Spark (1.6 preferred) from the official Apache Spark site; a prebuilt
version is available, just select from the provided dropdown "prebuilt for hadoop 2.6 or later".
The download url is: https://spark.apache.org/downloads.html. Extract the zip to a directory
of your choosing. The rest of the steps assume you have downloaded the prebuilt version.

2. Download ALL KDD Cup 2012 datasets: http://www.kddcup2012.org/c/kddcup2012-track1/data

3. Place all downloaded contents in a directory of your choosing (just so you know where it is)

4. Extract the track1.zip/track1.7z (either one) and also rec_log_test.txt.7z

5. Locate in our source code the file named "main.py", this should be in the current 
directory once you unzip OUR project source code. This will be our reference point from now on.

6. In the current directory (the one with main.py in it), locate the data/ directory and copy
"item.txt", "KDD_Track1_solution.csv", "rec_log_train.txt", "rec_log_test.txt", "user_action.txt",
"user_key_word.txt", "user_profile.txt", "user_sns.txt" to this data/ directory

7. Now, we are ready to run Spark!

8. Firstly, Spark's mllib library depends on the python module "numpy", so if you
do not have it installed, a "pip instal numpy" should do the trick.

9. Go to the directory that you got from Spark when you downloaded and unzipped
the prebuilt version. Ex: spark-1.6.0. To confirm, you should see directories such 
as: "assembly", "bagel", and "bin".

10. In this current location, create a folder "CS249_WCFF", and copy the entire
source code for our project (with the KDD files you added in step 6!) into
the CS249_WCFF folder

11. In the terminal, go to the spark-1.X.X/CS249_WCFF directory, where you can
"ls" and see "main.py".

12. Now we need to run Spark, simply run "../bin/pyspark" without quotes and Spark will output
some logging information. Once Spark stops printing to the shell, proceed to next
step.

13. Now spark is online and ready. Simply type in the shell: "execfile('main.py')"
without the quotes, and voila, the code will run.

14. Because of the time needed to run from scratch, we HIGHLY suggest that you
follow the Alternative steps, below.


############# 
## 2. Alternative: To make running faster, you can follow these below steps that
## will expedite runtime drastically. However, this requires more downloading - 
## no worries though, we will list links and walk you through everything!
#############

1. Follow steps 1-12 above
2. Now download... [todo]



