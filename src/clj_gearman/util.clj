(ns clj-gearman.util
  "Miscellaneous utility functions."
  (:import (java.nio ByteBuffer))
  (:require [clojure.pprint :refer [pprint]]))

(defn new-uniq
  "Generates a new version 4 UUID."
  []
  (str (java.util.UUID/randomUUID)))

(defn fun-arity
  "Returns the count of arguments that the function takes."
  [fun]
  (->> (class fun)
       .getDeclaredMethods
       (filter #(= "invoke" (.getName %)))
       first
       .getParameterTypes
       alength))

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
  "Converts a string in specific encoding to a byte array. Defaults to UTF-8."
  ([str] (str->bytea str "UTF-8"))
  ([str enc]
   (.getBytes str (or enc "UTF-8"))))

(defn bytea->str
  "Converts a byte array to a string in specific encoding. Defaults to UTF-8."
  ([byte-a] (bytea->str byte-a "UTF-8"))
  ([byte-a enc]
   (String. byte-a (or enc "UTF-8"))))


(defn concat-bytea
  "Concatenates a sequence of byte arrays into a single one."
  [byte-arrays]
  (let  [size (reduce + (map count byte-arrays))
         out (byte-array size)
         bb (ByteBuffer/wrap out)]
    (doseq [ba byte-arrays]
      (.put bb ba))
    out))

(defn msg->bytea
  "Converts a sequence of strings to a null-separated byte array."
  [msg enc]
  (if (empty? msg)
    (byte-array 0)
    (let [handle-args (mapv #(str->bytea (str %) "ASCII") (or (butlast msg) ()))
          data-arg    (str->bytea (str (last msg)) enc)]
      (->> data-arg
           (conj handle-args)
           (interpose (byte-array [(byte 0)]))
           concat-bytea))))

(defn- split-null
  "Splits byte-array to chunks separated by null bytes"
  [byte-a]
  (map byte-array (filter #(or (> (count %) 1) (not= (first %) (byte 0)))
                          (partition-by zero? byte-a))))

(defn bytea->msg
  "Converts a null-separated byte array to a sequence of strings."
  [byte-a enc]
  (if-not (empty? byte-a)
    (let [chunks (split-null byte-a)
          handle-args (if (< (count chunks) 2) [] (mapv #(bytea->str % "ASCII") (butlast chunks)))
          data-arg    (bytea->str (last chunks) enc)]
      (conj handle-args data-arg))
    []))
