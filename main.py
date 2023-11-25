from Searcher import Searcher

mySearcher = Searcher("DBforCrawlerNew.db")
queryString = "победа Россия"
mySearcher.getSortedList(queryString)
rowsLoc, wordsidList = mySearcher.getMatchRows(queryString)
print(f"{rowsLoc} {wordsidList}")
mySearcher.calculatePageRank()
mySearcher.pagerankScore(rowsLoc)
markedUrl = mySearcher.getScoredList(rowsLoc)
print(markedUrl)

markedHTMLFilename = "getMarkedHTML.html"

mySearcher.createMarkedHtmlFile(markedHTMLFilename, markedUrl, queryString.split())
