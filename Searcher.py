import sqlite3

class Searcher:
    def dbcommit(self):
        self.con.commit() # зафиксировать изменения в бд

    # конструктор
    def __init__(self,dbFileName):
        self.con = sqlite3.connect(dbFileName)
    
    # деструктор
    def __del__(self):
        self.con.close()

    def search(self, queryString, range):
        # приводим к нижнему регистру, получаем отдельные слова
        queryWordsList = self.getQueryWordsList(queryString)

        # получаем id слов в поисковой строке из таблицы wordList
        print("Список ID слов:")
        idList = self.getWordsIds(queryWordsList)

        # формируем таблицу, которая содержит все сочетания позиций всех слов поискового запроса
        # (строка табл: urlId, loc_queryWord1, loc_queryWord2)
        matchTable = self.getMatchRows(queryWordsList, idList)

        global dictUrl
        if (range == "by freq"):
            # РАНЖИРОВАНИЕ ПО ЧАСТОТЕ СЛОВ
            print("\nРанжирование по частоте слов")

            # словарь: ключ - idURL, значение – число строк с таким URL в matchTable
            dictUrl = self.getDictUrlByFreq(matchTable)

        if (range == "by page rank"):
            # РАНЖИРОВАНИЕ НА ОСНОВЕ ВНЕШНИХ ССЫЛОК
            print("\nРанжирование на основе внешних ссылок")

            # вычисляем ранги для всех страниц в БД (вероятность, что пользователь попадет на эту страницу)
            # self.calculatePageRank(20)

            dictUrl = self.getDictUrlByPageRank(matchTable)

        print("Количество уникальных URL, содержащих слова поискового запроса:")
        print(len(dictUrl))

        # приводим ранги к диапазону от 0.0 до 1.0
        normalizeDictURL = self.normalizeScores(dictUrl)

        # сортируем по убыванию
        sortedUrlList = self.getSortedList(normalizeDictURL)

        # печатаем результат
        self.printResult(sortedUrlList)

        # подкрашиваем слова в HTML
        self.createMarkedHtmlFile("HTML", sortedUrlList[0:3], queryWordsList)

    def getQueryWordsList(self, queryString):
        #queryString = queryString.lower()
        return queryString.split()
    
    # принимает строку поискового запроса и получает идентификаторы для каждого слова из БД
    def getWordsIds(self, queryString):
        #queryString = queryString.lower() # привести поисковый запрос к нижнему регистру
        queryWordsList = queryString.split() # разделить на отдельные слова (разделитель - пробел)
        rowidList = list() # список для хранения результата

        for word in queryWordsList:
            sql = "SELECT rowId FROM wordList WHERE word =\"{}\" LIMIT 1;".format(word)
            result_row = self.con.execute(sql).fetchone()

            if result_row != None:
                word_rowid = result_row[0]

                rowidList.append(word_rowid)
                print(" ", word, word_rowid)
            else:
                raise Exception("Одно из слов поискового запроса не найдено: " + word)
            
        return rowidList

    def getMatchRows(self, queryString):
        #queryString = queryString.lower()
        wordsList = queryString.split()

        wordsidList = self.getWordsIds(queryString)

        sqlFullQuery = """"""

        sqlpart_Name = list()
        sqlpart_Join = list()
        sqlpart_Condition = list()

        for wordIndex in range(0,len(wordsList)):
            wordID = wordsidList[wordIndex]

            if wordIndex == 0:
                sqlpart_Name.append("""w0.fk_URLId""")
                sqlpart_Name.append(""" , w0.location w0_loc""")
                sqlpart_Condition.append("""WHERE w0.fk_wordId={}""".format(wordID))
            else:
                if len(wordsList) >= 2:
                    sqlpart_Name.append(""" , w{}.location w{}_loc""".format(wordIndex,wordIndex))
                    sqlpart_Join.append("""INNER JOIN wordLocation w{} ON w0.fk_URLId=w{}.fk_URLId""".format(wordIndex, wordIndex, wordIndex))
                    sqlpart_Condition.append("""AND w{}.fk_wordId={}""".format(wordIndex, wordID, wordIndex))
                    pass
            pass
        
        sqlFullQuery += "SELECT "

        for sqlpart in sqlpart_Name:
            sqlFullQuery+="\n"
            sqlFullQuery += sqlpart

        sqlFullQuery += "\n"
        sqlFullQuery += "FROM wordLocation w0 "

        for sqlpart in sqlpart_Join:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        for sqlpart in sqlpart_Condition:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        #print(sqlFullQuery)
        cur = self.con.execute(sqlFullQuery)
        rows = [row for row in cur]
        print("Количество уникальных URL, содержащих слова поискового запроса:")
        distinct_url = dict()
        for row in rows:
            distinct_url[row[0]] = 0
        print(len(distinct_url))
        return rows, wordsidList
    
    # функция нормализации приводит ранги к диапазону от 0.0 до 1.0
    def normalizeScores(self, scores, smallIsBetter = 0):
        resultDict = dict()
        vsmall = 0.00001

        minscore = min(scores.values())
        maxscore = max(scores.values())

        for (key, val) in scores.items():
            if smallIsBetter:
                resultDict[key] = float(minscore) / max(vsmall, val)
            else:
                resultDict[key] = float(val) / maxscore
        return resultDict
    
    # ранжирование содержимого (частота слов)
    def frequencyScore(self, rowsLoc):
        # Создать countsDict - словарь с количеством упоминаний/комбинаций искомых слов -
        # {id URL страницы где встретилась комбинация искомых слов: общее количество комбинаций на странице }
        countsDict = dict()
        
        # поместить в словарь все ключи urlid с начальным значением счетчика "0"
        if len(rowsLoc) > 0:
            for location in rowsLoc:
                    countsDict[location[0]] = 0
            # Увеличивает счетчик для URLid +1 за каждую встреченную комбинацию искомых слов
            for location in rowsLoc:
                    countsDict[location[0]] += 1

        print(countsDict)
        # передать словарь счетчиков в функцию нормализации, режим "чем больше, тем лучше")
        return self.normalizeScores(countsDict, smallIsBetter=0)

    # выдача ранжированного результата
    def getUrlName(self, id):
        sql = "SELECT url FROM URLList WHERE rowId =\"{}\"".format(id)
        result_id = self.con.execute(sql).fetchone()
        return result_id

    # сортировка списка ранжированных url
    def getSortedList(self, queryString):
        rowsLoc, wordsidList = self.getMatchRows(queryString)
        m1Scores = self.frequencyScore(rowsLoc)
        
        rankedScoresList = list()
        for url, score in m1Scores.items():
            pair = (score, url)
            rankedScoresList.append(pair)

        rankedScoresList.sort(reverse = True)

        print("M1. РАНЖИРОВАНИЕ ПО СОДЕРЖИМОМУ (ЧАСТОТА СЛОВ)")
        print("score", "urlId", "getUrlName")
        for(score, urlid) in rankedScoresList[0:10]:
            print( "{:.2f} {:>5}  {}".format (score, urlid, self.getUrlName(urlid)))

    # ранжирование на основе внешних ссылок
    def calculatePageRank(self, iterations = 5):
        
        self.con.execute('DROP TABLE IF EXISTS pageRank')
        self.con.execute("""CREATE TABLE IF NOT EXISTS pageRank(
                                rowId INTEGER PRIMARY KEY AUTOINCREMENT,
                                urlId INTEGER,
                                score REAL
                        );""")
        
        self.con.execute("DROP INDEX IF EXISTS wordidx;")
        self.con.execute("DROP INDEX IF EXISTS urlidx;")
        self.con.execute("DROP INDEX IF EXISTS wordurlidx;")
        self.con.execute("DROP INDEX IF EXISTS urltoidx;")
        self.con.execute("DROP INDEX IF EXISTS urlfromidx;")

        self.con.execute('CREATE INDEX IF NOT EXISTS wordidx       ON wordList(word)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urlidx        ON URLList(url)')
        self.con.execute('CREATE INDEX IF NOT EXISTS wordurlidx    ON wordLocation(fk_wordId)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urltoidx      ON linkBetweenUrl(fk_ToURL_Id)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urlfromidx    ON linkBetweenUrl(fk_FromURL_Id)')
        
        self.con.execute("DROP INDEX   IF EXISTS rankurlididx;")
        self.con.execute('CREATE INDEX IF NOT EXISTS rankurlididx  ON pageRank(urlId)')
        
        self.con.execute("REINDEX wordidx;")
        self.con.execute("REINDEX urlidx;")
        self.con.execute("REINDEX wordurlidx;")

        self.con.execute("REINDEX urltoidx;")
        self.con.execute("REINDEX urlfromidx;")
        self.con.execute("REINDEX rankurlididx;")

        self.con.execute('INSERT INTO pageRank (urlId, score) SELECT rowId, 1 FROM URLList')
        self.dbcommit()

        for i in range(iterations):
            #print("Итерация %d" % (i))

            spisok_url = list()
            sql = "SELECT rowId FROM URLList"
            spisok_url = self.con.execute(sql).fetchall()

            for url in spisok_url:
                pr = 0.15

                spisokFromUrl = list()
                sql = "SELECT DISTINCT fk_FromURL_Id FROM linkBetweenURL WHERE fk_ToURL_Id={}".format(url[0])
                spisokFromUrl = self.con.execute(sql).fetchall()
                for fromUrl in spisokFromUrl:
                    linkingpr = self.con.execute("SELECT score FROM pageRank WHERE urlId={}".format(fromUrl[0])).fetchall()
                    linkingcount = self.con.execute("SELECT count(*) FROM linkBetweenURL WHERE fk_FromURL_Id={}".format(fromUrl[0])).fetchall()
                    pr += 0.85 * (linkingpr[0][0] / linkingcount[0][0])
                self.con.execute("UPDATE pageRank SET score={} WHERE urlId={}".format(pr, url[0]))
            self.dbcommit()
          
    def pagerankScore(self, rows):
        pagerank = dict()
        for row in rows:
            sql = "SELECT score FROM pageRank WHERE urlId={}".format(row[0])
            score = self.con.execute(sql).fetchall()
            pagerank[row[0]] = score[0][0]    
        maxrank = max(pagerank.values())

        for url, score in pagerank.items():
            pagerank[url] = score / maxrank
        
        normalizedscores = list()
        for url, score in pagerank.items():
                pair = (score, url)
                normalizedscores.append(pair)
        normalizedscores.sort(reverse = True)
        print("M2. РАНЖИРОВАНИЕ НА ОСНОВЕ ВНЕШНИХ ССЫЛОК (АЛГОРИТМ PAGERANK)")
        print("score", "urlId", "getUrlName")
        for(score, urlid) in normalizedscores[0:10]:
            print( "{:.2f} {:>5}  {}".format (score, urlid, self.getUrlName(urlid)))
        
        return pagerank

    def getScoredList(self, rowsLoc):
        totalScores = dict()
        for row in rowsLoc:
            totalScores[row[0]] = 0

        weights = [(0.5, self.frequencyScore(rowsLoc)),
                   (0.5, self.pagerankScore(rowsLoc))]
        
        for (weight, scores) in weights:
            for url in totalScores:
                totalScores[url] += weight * scores[url]

        m3Scores = list()
        for url, score in totalScores.items():
            pair = (score, url)
            m3Scores.append(pair)
        m3Scores.sort(reverse = True)
        print("M3. РАНЖИРОВАНИЕ ПО СОДЕРЖИМОМУ И НА ОСНОВЕ ВНЕШНИХ ССЫЛОК")
        print("score", "urlId", "getUrlName")
        for(score, urlid) in m3Scores[0:10]:
            print( "{:.2f} {:>5}  {}".format (score, urlid, self.getUrlName(urlid)))
        
        markedUrl = list()
        for scores, urlid in m3Scores[0:3]:
            markedUrl.append(urlid)
        return markedUrl

    def createMarkedHtmlFile(self, markedHTMLFilename, markedUrl, testQueryList):
       
        print(testQueryList)
        wordList = self.getWordList(markedUrl)
        htmlCode = self.getMarkedHTML(wordList, testQueryList)
        print(htmlCode)

        file = open(markedHTMLFilename, 'w', encoding="utf-8")
        file.write(htmlCode)
        file.close()
    
    def getMarkedHTML(self, wordList, queryList):
        resultHTML = "<html>"
        countUrl = 1
        for words in wordList:
            resultHTML += "<body>"
            resultHTML += "<h3>"
            resultHTML += "Страница № %d" %(countUrl)
            resultHTML += "</h3>"
            resultHTML += "<p>"
            for word in words:
                    if word[0] == queryList[0]:
                        resultHTML += '<span style="background-color:#CD5C5C">'
                        resultHTML += word[0]
                        resultHTML += '</span style="background-color:#CD5C5C">'
                    elif word[0] == queryList[1]:
                        resultHTML += '<span style="background-color:#4682B4">'
                        resultHTML += word[0]
                        resultHTML += '</span style="background-color:#4682B4">'
                    else:
                        resultHTML += word[0]
                    resultHTML += " "
            resultHTML += "</p>"
            resultHTML += "</body>"
            resultHTML += "</br>"
            countUrl += 1
        resultHTML += "</html>"
        return resultHTML

    def getWordList(self, markedUrl):
        wordList = list()
        for url in markedUrl:
            sql = "SELECT word FROM wordList INNER JOIN wordLocation ON wordList.rowId = wordLocation.fk_wordId WHERE wordLocation.fk_URLId = {}".format(url)
            wordList.append(self.con.execute(sql).fetchall())
        return wordList