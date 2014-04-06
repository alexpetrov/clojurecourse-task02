(ns task02.Database
  (:use [task02 db query])
  ;; Объявить класс task02.Database с двумя статическими функциями доступными из Java:
  ;;  - void InitDatabase() - должна выполнять начальную загрузку данных используя функцию
  ;;       task02.db/load-initial-data
  ;;  - String Select(String query) - должна выполнять запрос к базе данных
  ;;       (task02.query/perform-query) и возвращать результат в виде строки в формате EDN.

  ;; Hint: load-initial-data, pr-str, perform-query
  ; :implement-me
  (:gen-class))

(defn ^:static -InitDatabase []
  (load-initial-data))

(defn ^:static -Select [^String query]
  (perform-query query))
