(ns clj-gearman.pool
  "Gearman job server connection pool functions."
  (:import (java.net ConnectException))
  (:require [clj-gearman.socket :as s]))


(defn- conn-socket [meta retry]
  (fn [conn] (try (s/connect conn meta retry) (catch ConnectException ex nil))))

(defn- conn-slow [servers meta]
  (if-let [socket (some (conn-socket meta 10) (butlast servers))]
    socket
    (s/connect (last servers) meta 10)))

(defn- conn-fast [servers meta]
  (some (conn-socket meta 0) servers))

(defn connect-first
  "Loop through a list of servers and return first one available"
  [servers conn-meta]
  ; First try each server without retries.
  (if-let [socket (conn-fast servers conn-meta)]
    socket
    ; If that fails, loop them again and throw exception if we still can't connect.
    (conn-slow servers conn-meta)))
