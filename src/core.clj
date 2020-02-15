(ns core
  (:require
   [datahike.api :as d]))

(def counter (atom 0))
(def error-count (atom 0))

(def db-uri "datahike:file:///tmp/demo-db1")

(d/delete-database db-uri)
(d/create-database db-uri)
(def conn (d/connect db-uri))

(def schema   #:db{:valueType :db.type/uuid, :cardinality :db.cardinality/many,
                   :ident :uuid, :index true, :unique :db.unique/identity})

(d/transact conn [schema])

(defn random-entity [] {:uuid (java.util.UUID/randomUUID)})

(comment "to see the entities"
         (take 4 (repeatedly random-entity)))

(defn thousand-things []
  (vec (take 1000 (repeatedly random-entity))))

(defn pull-uuid [conn lid]
  (swap! counter inc)
  (try
    (d/pull @conn '[*] [:uuid (first lid)])
    (catch Exception e (do
                         (swap! error-count inc)
                         (println (str "nr "@counter " error " @error-count " "
                                       (.getMessage e) " for " (second lid)))))))

(d/transact conn (thousand-things))

(def uuids (d/q '[:find ?lid ?e
                  :where
                  [?e :uuid ?lid]]
                @conn))

(count uuids)

(run! (partial pull-uuid conn) uuids)
