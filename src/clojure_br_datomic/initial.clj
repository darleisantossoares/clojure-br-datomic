(ns clojure-br-datomic.initial
  (:require [datomic.api :as d]
            [clj-http.client :as http]
            [cheshire.core :as json]))

(def uri "datomic:dev://localhost:4334/nba")

(d/create-database uri)

(def conn (d/connect uri))

(def player-schema
  [{:db/ident       :player/id
    :db/valueType   :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "Unique identifier for a player"}

   {:db/ident       :player/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "The full name of the player"}

   {:db/ident       :player/position
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc         "Position of the player (e.g., :guard, :forward, :center)"}

   {:db/ident       :player/team
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Reference to the team entity the player belongs to"}

   {:db/ident       :player/stats
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Map of player statistics for the season"}])


(def team-schema
  [{:db/ident       :team/id
    :db/valueType   :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "Unique identifier for a team"}

   {:db/ident       :team/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Name of the team"}

   {:db/ident       :team/city
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "City of the team"}

   {:db/ident       :team/roster
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc         "References to players on the team roster"}

   {:db/ident       :team/stats
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Map of team statistics for the season"}])


(def game-schema
  [{:db/ident       :game/id
    :db/valueType   :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "Unique identifier for a game"}

   {:db/ident       :game/date
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc         "Date of the game"}

   {:db/ident       :game/location
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Location where the game took place"}

   {:db/ident       :game/home-team
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Reference to the home team"}

   {:db/ident       :game/away-team
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Reference to the away team"}

   {:db/ident       :game/home-score
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "Score of the home team"}

   {:db/ident       :game/away-score
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "Score of the away team"}

   {:db/ident       :game/player-stats
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc         "References to PlayerGameStats entities"}])


(def player-game-stats-schema
  [{:db/ident       :player-game-stats/player
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Reference to the player"}

   {:db/ident       :player-game-stats/game
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Reference to the game"}

   {:db/ident       :player-game-stats/points
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "Points scored by the player in the game"}

   {:db/ident       :player-game-stats/assists
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "Assists made by the player in the game"}

   {:db/ident       :player-game-stats/rebounds
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "Rebounds collected by the player in the game"}
   ])

@(d/transact conn player-schema)


@(d/transact conn (concat team-schema game-schema player-game-stats-schema))



;;; getting data from the api

(def api-base "https://api.balldontlie.io/v1")

#_(defn get-json [url]
  (-> (http/get url {:as :json})
      :body))

#_(defn get-players []
  (get-json (str api-base "/players")))

#_(defn get-teams []
  (let [response (http/get (str api-base "/teams") {:as :json})]
    (println "Status:" (:status response))
    (println "Body:" (:body response))
    (:body response)))

#_(defn get-teams []
  (get-json (str api-base "/teams")))

#_(defn get-games []
  (get-json (str api-base "/games?seasons[]=2022")))



(def personal-token (System/getenv "balldontlietoken"))



(defn get-teams [token]
  (let [response (http/get (str api-base "/teams")
                           {:headers {"Authorization" token}
                            :as :json})]
    (if (= 200 (:status response))
      (:body response)
      (throw (ex-info "Failed to fetch teams" {:status (:status response) :body (:body response)})))))


(defn get-players [token]
  (let [response (http/get (str api-base "/players")
                           {:headers {"Authorization" token}
                            :as :json})]
    (if (= 200 (:status response))
      (:body response)
      (throw (ex-info "Failed to fetch players" {:status (:status response) :body (:body response)})))))

(println (get-players personal-token))

(defn get-players
  ([token]
   (get-players token nil [] 0))
  ([token next-cursor acc total]
   (let [url (str api-base "/players" (when next-cursor (str "?page=" next-cursor)))
         response (http/get url {:headers {"Authorization" token} :as :json})
         data (:body response)
         players (:data data)
         next-cursor (get-in response [:body :meta :next_cursor])]
     (println "Fetched data:" data)
     (println "Sleeping for 5 seconds..." total)
     (Thread/sleep 5000) ;; Sleep after the request
     (if (= 200 (:status response))
       (if next-cursor
         (recur token next-cursor (concat acc players) (inc total))
         (concat acc players))
       (throw (ex-info "Failed to fetch players" {:status (:status response) :body (:body response)}))))))


(println "###########")

(get-players personal-token)
(count (get-players personal-token))

(clojure.pprint/pprint  (get-players personal-token))

(map (fn [team] @(d/transact conn [(transform-team team)])) (filter (fn [team] (not= (:city team) "")) (:data (get-teams personal-token))))

(def players (get-players personal-token))

(println (get-players personal-token))

(count players)

(defn transform-player [player-data]
  {:player/id     (java.util.UUID/randomUUID)
   :player/name   (str (:first_name player-data) " " (:last_name player-data))
   :player/position (:position player-data)
   :player/team   [:team/id (get-in player-data [:team :id])]})

(defn transform-team [team-data]
  {:team/id   (d/squuid)
   :team/name (:full_name team-data)
   :team/city (:city team-data)
   :team/roster []})

(defn transform-game [game-data]
  {:game/id       (java.util.UUID/randomUUID)
   :game/date     (java.time.Instant/parse (:date game-data))
   :game/location (:arena game-data)
   :game/home-team [:team/id (:home_team_id game-data)]
   :game/away-team [:team/id (:visitor_team_id game-data)]
   :game/home-score (:home_team_score game-data)
   :game/away-score (:visitor_team_score game-data)
   :game/player-stats []})

(map (fn [player] @(d/transact conn [(transform-player player)])) players)

(map transform-team (get-teams))

