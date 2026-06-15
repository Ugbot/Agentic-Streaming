(ns agentic.classifier
  "Classifier SPI — formalizes the existing lexicon classifier as a protocol so the
   Clojure core matches the Classifier SPI exposed by the other cores. Wraps the
   existing `agentic.guardrail/lexicon-classify`; behaviour unchanged."
  (:require [agentic.guardrail :as guardrail]))

(defprotocol Classifier
  "Classifies text into a label with a confidence score and a full label→score map."
  (classify [_ text] "Classify text → {:label :score :scores} (:scores is label→score)."))

(defrecord LexiconClassifier [lexicon default-label]
  Classifier
  ;; lexicon-classify already returns {:label :score :scores}, so delegate directly.
  (classify [_ text] (guardrail/lexicon-classify lexicon default-label text)))

(defn lexicon-classifier
  "A Classifier backed by the existing keyword-weighted lexicon classifier
   (`agentic.guardrail/lexicon-classify`). `lexicon` is a map of label → seq of words;
   `default-label` is returned when no words match / text is empty."
  [lexicon default-label]
  (->LexiconClassifier lexicon default-label))
