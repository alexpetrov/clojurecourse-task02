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

;; > (parse-select "select student where id = 10 order by id limit 2 join subject on id = sid join subject2 on id = sid" )
;; ("student" :where #<function> :order-by :id :limit 2 :joins [[:id "subject" :sid]])
;; > (parse-select "werfwefw")
;; nil
(def sign->fn
  {:= =
   :> >
   :< <
   :<= <=
   :>= >=
   :!= not=})

(defn prepare-value-for-where [value]
  (if (re-matches #"'.*'" value)
    value
    (parse-int value)))
;; (re-matches #"'.*'" "'10'")
;; (prepare-value-for-where "10")

(defn make-where-function [first sign second]
   #(((keyword sign) sign->fn) ((keyword first) %) (prepare-value-for-where second)))

;; (parse-join (vec (.split "join subject on id = sid join subject1 on id = sid" " ")))
;; (parse-joins (vec (.split "join subject on id = sid join subject1 on id = sid join subject2 on id = sid" " ")))

(defn parse-joins [query]
  (match query
     ["join" entity "on" field1 "=" field2 & _]
         (-> (parse-joins (vec (drop 6 query)))
             (conj [(keyword field1) entity (keyword field2)]))
         :else ()))

(defn parse-join [query]
  (match query
         ["join" & _] (into [] (parse-joins query))))

(defn parse-query [query]
  (match query
     ["select" tbl & _]
         (-> (parse-query (vec (drop 2 query)))
             (conj tbl))
     ["where" first sign second & _]
         (-> (parse-query (vec (drop 4 query)))
             (conj (make-where-function first sign second))
             (conj :where))
     ["order" "by" field & _]
         (-> (parse-query (vec (drop 3 query)))
             (conj (keyword field))
             (conj :order-by))
     ["limit" limit & _]
         (-> (parse-query (vec (drop 2 query)))
             (conj (parse-int limit))
             (conj :limit))
     ["join" & _]
        (-> (parse-join query)
            (list)
            (conj :joins))
     :else nil))

;; > (parse-select "select student")

(def special-words #{"select" "where" "order" "by" "limit" "join"})

(defn downcase-special-words [query]
  (for [elem query]
    (if (contains? special-words (.toLowerCase elem))
      (.toLowerCase elem)
      elem)))

(defn parse-select [^String sel-string]
  (let [query (vec (downcase-special-words (.split sel-string " ")))]
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
