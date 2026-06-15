(ns agentic.retrieval
  "Hot+cold retrieval with the deterministic FNV-1a hashing embedder — byte-compatible with the
   Python/Go/Java cores (same FNV constants, tokenization, bucketing, L2-normalization), so vectors
   and routing match across languages. Model-free."
  (:require [clojure.string :as str]))

(def ^:const fnv-offset 0x811C9DC5)
(def ^:const fnv-prime 0x01000193)
(def ^:const mask-32 0xFFFFFFFF)

(defn fnv1a-32
  "FNV-1a 32-bit hash over the UTF-8 bytes of token (returned as an unsigned long 0..2^32-1)."
  [^String token]
  (let [bytes (.getBytes token "UTF-8")
        n (alength bytes)]
    (loop [i 0 h (long fnv-offset)]
      (if (< i n)
        (let [b (bit-and (long (aget bytes i)) 0xFF)
              h (bit-and (* (bit-xor h b) fnv-prime) mask-32)]
          (recur (inc i) h))
        h))))

(defn embed
  "Bag-of-words hashing embedder, L2-normalized — a vector of dim doubles."
  [text dim]
  (let [v (double-array dim)]
    (when text
      (doseq [tok (str/split (str/lower-case text) #"[^a-z0-9]+")]
        (when (seq tok)
          (let [bucket (int (mod (fnv1a-32 tok) dim))]
            (aset v bucket (+ (aget v bucket) 1.0))))))
    (let [norm (Math/sqrt (areduce v i acc 0.0 (+ acc (* (aget v i) (aget v i)))))]
      (when (pos? norm)
        (dotimes [i dim] (aset v i (/ (aget v i) norm))))
      (vec v))))

(defn cosine [a b]
  (if (not= (count a) (count b))
    -1.0
    (let [dot (reduce + (map * a b))
          na (Math/sqrt (reduce + (map #(* % %) a)))
          nb (Math/sqrt (reduce + (map #(* % %) b)))]
      (if (or (zero? na) (zero? nb)) 0.0 (/ dot (* na nb))))))

;; ---- hot index + two-tier retriever ----

(defn hot-index [] (atom {}))

(defn upsert [idx id vec text]
  (swap! idx assoc id {:vec vec :text text}))

(defn search [idx query k]
  (->> @idx
       (map (fn [[id {:keys [vec text]}]] {:id id :score (cosine query vec) :text text}))
       (sort-by :score >)
       (take (max 1 k))
       vec))

(defn two-tier
  "A retriever merging a hot index and an optional cold search fn (cold = nil for hot-only)."
  [hot cold hot-k cold-k]
  {:hot hot :cold cold :hot-k hot-k :cold-k cold-k})

(defn retrieve [retriever query k]
  (let [hot-hits (when (:hot retriever) (search (:hot retriever) query (:hot-k retriever)))
        cold-hits (when (:cold retriever) ((:cold retriever) query (:cold-k retriever)))
        merged (->> (concat hot-hits cold-hits)
                    (group-by :id)
                    (map (fn [[_ hits]] (apply max-key :score hits)))  ; hot wins ties via max score
                    (sort-by :score >)
                    (take (max 1 k)))]
    (vec merged)))
