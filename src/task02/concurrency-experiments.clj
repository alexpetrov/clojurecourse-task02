(ns concurrency-experiments
  "Experiments with concurrency")

;; Refs
(defn transfer-money [from to amount]
  (dosync
    (if (< @from amount)
       (throw (IllegalStateException.
          (str "Account has less money that required!" @from "<" amount)))
       (do (alter from - amount) (alter to + amount)))))

(def ^:private acc-1 (ref 1000))
(def ^:private acc-2 (ref 1000))

(transfer-money acc-1 acc-2 700)

@acc-1
@acc-2

(defn add-to-deposit [to amount]
  (dosync
   (commute to + amount)))

(add-to-deposit acc-1 300)

(defn write-log [log-msg]
  (io!
   (println log-msg)))

;;(dosync (write-log "test"))

;; Atoms
(def ^:private counters-atom (atom {}))

(defn inc-counter [name]
  (swap! counters-atom update-in [name] (fnil inc 0)))

(defn dec-counter [name]
  (swap! counters-atom update-in [name] (fnil dec 0)))

(defn reset-counter [name]
  (swap! counters-atom assoc name 0))

@counters-atom

(inc-counter :test)

(inc-counter :another-test)

(reset-counter :test)

(dec-counter :another-test)

;; agents
(def ^:private counters-agent (agent {}))

(defn a-inc-counter [name]
  (send counters-agent update-in [name] (fnil inc 0)))

(defn a-dec-counter [name]
  (send counters-agent update-in [name] (fnil dec 0)))

(defn a-reset-counter [name]
  (send counters-agent assoc name 0))

(defn long-running-assoc[m name value] (do (Thread/sleep 5000)(assoc m name value)))

(defn a-long-change-counter [name]
  (send counters-agent long-running-assoc name 999))

@counters-agent

(a-inc-counter :test)

(a-long-change-counter :test)

(a-reset-counter :test)

(send counters-agent long-running-assoc name 999)
(a-dec-counter :test)

(def err-agent (agent 1))

(send err-agent (fn [_] (throw (Exception. "we have a problem!"))))

(send err-agent identity)

(def err-agent (agent 1 :error-mode :continue))

(send err-agent inc)

@err-agent

(def data (atom []))
(swap! data into [1 2 3])
(into [1 2 3] [3 4 6])
@data
