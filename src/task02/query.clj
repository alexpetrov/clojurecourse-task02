(ns task02.query
  (:use [task02 helpers db])
  (:use [clojure.core.match :only (match)])
)

;; Функция выполняющая парсинг запроса переданного пользователем
;;
;; Синтаксис запроса:
;; SELECT table_name [WHERE column comp-op value] [ORDER BY column] [LIMIT N] [JOIN other_table ON left_column = right_column]
;;
;; - Имена колонок указываются в запросе как обычные имена, а не в виде keywords. В
;;   результате необходимо преобразовать в keywords
;; - Поддерживаемые операторы WHERE: =, !=, <, >, <=, >=
;; - Имя таблицы в JOIN указывается в виде строки - оно будет передано функции get-table для получения объекта
;; - Значение value может быть либо числом, либо строкой в одинарных кавычках ('test').
;;   Необходимо вернуть данные соответствующего типа, удалив одинарные кавычки для строки.
;;
;; - Ключевые слова --> case-insensitive
;;
;; Функция должна вернуть последовательность со следующей структурой:
;;  - имя таблицы в виде строки
;;  - остальные параметры которые будут переданы select
;;
;; Если запрос нельзя распарсить, то вернуть nil

;; Примеры вызова:
;; > (parse-select "select student")
;; ("student")
;; > (parse-select "select student where id = 10")
;; ("student" :where #<function>)
;; > (parse-select "select student where id = 10 limit 2")
;; ("student" :where #<function> :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2")
;; ("student" :where #<function> :order-by :id :limit 2)
;; > (parse-select "select student where id = 10 order by id")

;; > (parse-select "select student where id = 10 order by id limit 2 join subject on id = sid")
;; ("student" :where #<function> :order-by :id :limit 2 :joins [[:id "subject" :sid]])
;; > (parse-select "werfwefw")
;; nil
(def sign->fn
  {:= =
   :> >
   :< <
   :!= not=})


(def req (vec (.split "select students where id > 10" " ")))

(defn make-where-function [first sign second]
   #(((keyword sign) sign->fn) ((keyword first) %) second))
;; (parse-joins (vec (.split "join subject on id = sid join subject on id = sid" " ")))

(defn parse-joins [query]
  ()
  #_(match query
       ["join" entity "on" field1 "=" field2 & _] (vector (vector (keyword field1) entity (keyword field2)) (parse-joins (vec (drop 6 query))))
       :else ()))

(defn parse-limit [query]
  (match query
         ["limit" limit & _]
         (identity (list :limit (parse-int limit)
                        (parse-joins (vec (drop 2 query)))))
         :else ()))

(defn parse-order-by [query]
  (match query
         ["order" "by" field & _]
         (identity (list :order-by (keyword field)
                        (parse-limit (vec (drop 3 query)))))
         :else ()))


;; (parse-where-clause ["where" "id" "=" "sid"])
(defn parse-where-clause [query]
  (match query
         ["where" first sign second & _] (identity (list :where (make-where-function first sign second) (parse-order-by (vec (drop 4 query)))))
         :else ()))

(defn parse-query [query]
  (match query
         ["select" tbl & _] (flatten (list tbl (parse-where-clause (vec (drop 2 query)))))
         :else ()))


(defn parse-select [^String sel-string]
  (let [query (vec (.split sel-string " "))]
    (parse-query query)))

;; Выполняет запрос переданный в строке.  Бросает исключение если не удалось распарсить запрос

;; Примеры вызова:
;; > (perform-query "select student")
;; ({:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "select student order by year")
;; ({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"} {:id 1, :year 1998, :surname "Ivanov"})
;; > (perform-query "select student where id > 1")
;; ({:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "not valid")
;; exception...
(defn perform-query [^String sel-string]
  (if-let [query (parse-select sel-string)]
    (apply select (get-table (first query)) (rest query))
    (throw (IllegalArgumentException. (str "Can't parse query: " sel-string)))))
