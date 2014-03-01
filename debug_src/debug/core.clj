(ns debug.core
 (:require [potemkin :as po]
           [clojure.tools.trace :as tools_trace]
           [environ.core :as env_c]
           [clojure.tools.nrepl.server :as nrepl]
           ))  

; nREPL server 
(defn- startServer 
 "Starts a nREPL server so you can do a 
  :Connect using vim fireplace and get access
  to all the running image
  - see the project.clj file env keys to
  determine what happens when the server is
  started."
[]
 (if (true? (env_c/env :nrepl-server-create)) 
  (let [port (env_c/env :nrepl-server-port)] 
   (do 
    (println *ns* "create nrepl server on port" port)
    (nrepl/start-server :port port))) 
  ))
(startServer)

; Debug configuration
(defn- configDebug 
 "Add this to the require of clj files that you want 
   to use the debug routines
   [debug.core :as debug]
  Use this function to expose debug tool specific items
   under this namespace - ala potemkin"
 []
 (po/import-vars [tools_trace trace deftrace trace-forms trace-ns trace-vars])
 (println *ns* "config"))
(configDebug)

