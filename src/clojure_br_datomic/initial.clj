(ns clojure-br-datomic.initial
  (:require [datomic.api :as d]))

(def uri "datomic:dev://localhost:4334/nba")

(d/create-database uri)

(def conn (d/connect uri))







