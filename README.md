# BigDataProject

The purpose of this project is to obtain the top rated pages from the list of pages in the given set of Wikipedia Articles. Amazon Elastic MapReduce (EMR) is used to implement the PAGERANK Algorithm on the provided Wikipedia Dataset.

PageRank is a significant part of search engine optimization and is a link analysis algorithm applied by GOOGLE. It assigns a rank to each accessed webpage. 
 Evaluating PageRank involves evaluating all the links to the specific page. If a page is called or accessed by a lot of links then it will be ranked high among all searched pages.
 
 
 xmlDriver1.java - This is the main class of mapReduce.

xmlMapper.java - This is the mapper class and it extracts the contents of the xml file within title and revision tags. The output of this class is  in page_title and page_link) format.

XmlInputFormat.java - This class reads the XML file.

RedLinks.pig - This script extracts the redlinks from the output file of the driver class.

PageRankCount.pig - This pig script calculates the page rank for each pages for 10 iterations.

Final_Count.pig - This script extracts the universities names starts with "university of" and ends with "university" and also it extracts the top 100 universities.









