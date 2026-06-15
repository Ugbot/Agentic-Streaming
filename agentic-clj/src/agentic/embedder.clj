(ns agentic.embedder
  "Embedder SPI — formalizes the existing hashing embedder as a protocol so the
   Clojure core matches the Embedder SPI exposed by the other cores. Wraps the
   existing `agentic.retrieval/embed` (FNV-1a hashing embedder); behaviour unchanged."
  (:require [agentic.retrieval :as retrieval]))

(def ^:const default-dim 256)

(defprotocol Embedder
  "Turns text into a dense vector and batches of text into vectors of vectors."
  (embed-text [_ text] "Embed a single string → a vector of doubles.")
  (embed-batch [_ texts] "Embed a sequence of strings → a vector of vectors of doubles."))

(defrecord HashingEmbedder [dim]
  Embedder
  (embed-text [_ text] (retrieval/embed text dim))
  (embed-batch [this texts] (mapv #(embed-text this %) texts)))

(defn hashing-embedder
  "An Embedder backed by the existing FNV-1a hashing embedder (`agentic.retrieval/embed`).
   Defaults to `default-dim` dimensions."
  ([] (hashing-embedder default-dim))
  ([dim] (->HashingEmbedder dim)))
