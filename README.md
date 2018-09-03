# BigDataProject

Introduction:

The purpose of this project is to obtain the top rated pages from the list of pages in the given set of Wikipedia Articles. In order to implement, the PAGERANK Algorithm is used.

PageRank is a significant part of search engine optimization and is a link analysis algorithm applied by GOOGLE. It assigns a rank to each accessed webpage. 
 Evaluating PageRank involves evaluating all the links to the specific page. If a page is called or accessed by a lot of links then it will be ranked high among all searched pages.

PageRank can be calculated using the formula:

Where


	
  d: is a constant (it is typically set to 0.85) 
	
  N: is total number of pages ( In this case Wikipedia articles)
	
  in(p_i ): The set of all incoming links to p_i
	
  L(p_j): The number of outgoing links form p_j

Initially, each page is initialized with PageRank 1/N and the sum of the PageRank is 1. 

Example :

We regard a smal l web consisting of three pages A, B and C, whereby page A links to the pages B and C, page B links to page C and page C 
links to page A. The following figure illustrates the link graph of this simple problem. 






