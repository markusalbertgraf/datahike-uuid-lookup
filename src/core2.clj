(ns core2
  (:require
   [datahike.api :as d]
   [datahike-leveldb.core]
   [com.hypirion.clj-xchart :as c]))

(def counter (atom 0))
(def error-count (atom 0))

(def db-uri-m "datahike:mem:///demo-db1")
(def db-uri-f "datahike:file:///tmp/demo-db1")
(def db-uri-l "datahike:level:///tmp/demo-devel-db1")

(def schema   #:db{:valueType :db.type/uuid, :cardinality :db.cardinality/one,
                   :ident :uuid, :index true, :unique :db.unique/identity})

(defn squuid []
  (let [uuid (java.util.UUID/randomUUID)
        time (System/currentTimeMillis)
        secs (quot time 1000)
        lsb  (.getLeastSignificantBits uuid)
        msb  (.getMostSignificantBits uuid)
        time-msb (bit-or (bit-shift-left secs 32)
                         (bit-and 0x00000000ffffffff msb))]
    (java.util.UUID. time-msb lsb)))

(defn random-entity [] {:uuid (java.util.UUID/randomUUID)})

(defn sequential-random-entity [] {:uuid (squuid)})

(defn n-random-things [n]
  (vec (take n (repeatedly random-entity))))

(defn n-sequential-things [n]
  (vec (take n (repeatedly sequential-random-entity))))


(defn pull-uuid [conn lid]
  (swap! counter inc)
  (try
    (do
      #_(print @counter " " #_lid #_" ")
      (d/pull @conn '[*] [:uuid (first lid)]))
    (catch Exception e (do
                         (swap! error-count inc)
                         #_(println (str "\nnr "@counter " error " @error-count " "
                                         (.getMessage e) " :db/id " (second lid)))))))

(defn uuids [conn]
  (d/q '[:find ?lid ?e
         :where
         [?e :uuid ?lid]]
       @conn))

(defn test-uuid
  "takes a connection, a function to generate entities, the nr of entities to generate"
  [uri create-fn nr]
  #_(println "\n-------------------------------------------------------------------")
  #_(println "testing uri " uri " fn " create-fn)
  (reset! counter 0)
  (reset! error-count 0)
  (d/delete-database uri)
  (d/create-database uri)
  (let [conn  (d/connect uri)
        _     (d/transact conn [schema])
        _     (d/transact conn (create-fn nr))
        uuids (d/q '[:find ?lid ?e
                     :where
                     [?e :uuid ?lid]]
                   @conn)]
    (run! (partial pull-uuid conn) uuids)
    #_(println)
    (println "uri " uri " fn " create-fn " errors: " @error-count " of " @counter)
    @error-count))


(comment
  (def result4
    (for [n (range 10 10000 30)]
      (let [m (test-uuid db-uri-m n-random-things n)
            f (test-uuid db-uri-f n-random-things n)
            l (test-uuid db-uri-l n-random-things n)]
        {:nr n
         :mem m
         :file f
         :level l})))

  )

(defn plot [result]
  (c/view
   (c/xy-chart
    {"mem"   {:x (map :nr result)
              :y (map #(* 100 (/ (:mem %) (:nr %)))  result)}
     "file"  {:x (map :nr result)
              :y (map #(* 100 (/ (:file %) (:nr %))) result)}
     "level" {:x (map :nr result)
              :y (map #(* 100 (/ (:level %) (:nr %))) result)}}
    {:title  "Fehlerhäufigkeit nach Datenbankgröße"
     :x-axis {:title "Anzahl Entities in DB"}
     :y-axis {:title "Fehler in %"}})))

(defn chart [result]
  (c/xy-chart
   {"mem"   {:x (map :nr result)
             :y (map #(* 100 (/ (:mem %) (:nr %)))  result)}
    "file"  {:x (map :nr result)
             :y (map #(* 100 (/ (:file %) (:nr %))) result)}
    "level" {:x (map :nr result)
             :y (map #(* 100 (/ (:level %) (:nr %))) result)}}
   {:title  "Fehlerhäufigkeit nach Datenbankgröße"
    :width  2048
    :height 1000
    :x-axis {:title "Anzahl Entities in DB"}
    :y-axis {:title "Fehler in %"}}))

(comment
  (plot result4)

  (c/spit (chart result3) "Fehlerhäufigkeit-2048-1000.svg")
  (c/spit (chart result4) "Fehlerhäufigkeit-step30to10000.svg")


  (spit "result3.edn" (vec result3))


  )



#_(for [n (range 10 1000 10)]
    (do
      (test-uuid db-uri-m n-random-things n)
      (test-uuid db-uri-f n-random-things n)
      (test-uuid db-uri-l n-random-things n)
      (test-uuid db-uri-m n-sequential-things n)
      (test-uuid db-uri-f n-sequential-things n)
      (test-uuid db-uri-l n-sequential-things n)))


