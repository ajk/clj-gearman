(ns clj-gearman.socket
  (:import (java.net Socket SocketTimeoutException)
           (java.io Closeable))
  (:require [clj-gearman.header :as h]
            [clj-gearman.util :as u]))

; We need to use a stupid wrapper in order to pile our
; client and worker metadata on the socket.
(deftype MetaSocket [socket _meta]

  clojure.lang.IObj
  (meta [_] _meta)
  (withMeta [_ m] (MetaSocket. socket m))

  clojure.lang.IDeref
  (deref [_] socket)

  java.io.Closeable
  (close [_] (.close socket))

  Object
  (toString [this]
    (str (.getName (class this))
         ": "
         (pr-str socket))))


(defn connect
  ([conn] (connect conn {}))
  ([{:keys [host port]} meta]
   (MetaSocket. (Socket. host port) meta)))

(defn disconnect [sock]
  (.close @sock))

(defn read-bytes [in ^Integer len]
  (if (pos? len)
    (byte-array (map (fn [_] (.read in)) (range len)))))

(defn read-int [in]
  (u/bytea->int (read-bytes in 4)))

(defn read-msg [sock enc]
  (let [r      (.getInputStream @sock)
        type   (read-bytes r 4)
        code   (read-int r)
        size   (read-int r)
        byte-a (read-bytes r size)]
    (list type code size (u/bytea->msg byte-a enc))))

(defn write-msg [sock type code enc msg]
  (let [w    (.getOutputStream @sock)
        data (u/msg->bytea msg enc)
        size (count data)]
    (.write w (u/concat-bytea
                [type
                 (u/int->bytea code)
                 (u/int->bytea size)
                 data]))
    (.flush w)))

(defn wait-loop [worker fun]
  (.setSoTimeout @worker 30000)
  (loop []
    (if-let [res (try (fun) (catch SocketTimeoutException _ false))]
      (do
        (.setSoTimeout @worker 0)
        res)
      (recur))))

(defn response [socket opt]
  (let [[_ code _ data] (read-msg socket (:in-enc opt))]
    [(h/lookup code) data]))

(defn request [socket code & msg]
  (let [opt (meta socket)]
    (write-msg socket h/req code (:out-enc opt) msg)
    (response socket opt)))
