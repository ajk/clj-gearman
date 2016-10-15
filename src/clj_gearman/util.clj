(ns clj-gearman.util
  (:import (java.nio ByteBuffer))
  (:require [clojure.pprint :refer [pprint]]))


(defn int->bytea
  "Converts signed integer to a byte array in big-endian order."
  [x]
  (-> (ByteBuffer/allocate 4)
      (.putInt x)
      (.array)))

(defn bytea->int
  "Converts a byte array in big-endian order to signed integer."
  [byte-a]
  (-> (ByteBuffer/allocate 4)
      (.put byte-a)
      (.getInt 0)))

(defn str->bytea
  "Converts a string in specific encoding to a byte array."
  ([str] (str->bytea str "UTF-8"))
  ([str enc]
   (.getBytes str (or enc "UTF-8"))))

(defn bytea->str
  "Converts a byte array to a string in specific encoding."
  ([byte-a] (bytea->str byte-a "UTF-8"))
  ([byte-a enc]
   (String. byte-a (or enc "UTF-8"))))

(defn msg->bytea
  "Converts a sequence of strings to a null-separated byte array."
  [msg enc]
  (if (empty? msg)
    []
    (let [handle-args (mapv #(str->bytea % "ASCII") (or (butlast msg) ()))
          data-arg    (str->bytea (last msg) enc)]
      (interpose (byte-array [(byte 0)]) (conj handle-args data-arg)))))

(defn split-null
  "Splits byte-array to chunks separated by null bytes"
  [byte-a]
  (map byte-array (filter #(or (> (count %) 1) (not= (first %) (byte 0)))
                          (partition-by #(= % 0) byte-a))))

(defn bytea->msg
  "Converts a null-separated byte array to a sequence of strings."
  [byte-a enc]
  (if-not (empty? byte-a)
    (let [chunks (split-null byte-a)
          handle-args (if (< (count chunks) 2) [] (mapv #(bytea->str % "ASCII") (butlast chunks)))
          data-arg    (bytea->str (last chunks) enc)]
      (conj handle-args data-arg))
    []))
